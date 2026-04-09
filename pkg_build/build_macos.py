#!/usr/bin/env python3
"""Build a macOS Bani.app bundle and .dmg installer.

Creates a self-contained Bani.app that includes:
- Bundled Python runtime
- All Bani dependencies
- Pre-built React UI
- Menu bar launcher

The .dmg is a drag-to-install disk image.

Usage:
    PYTHONPATH=. python -m pkg_build.build_macos
"""

from __future__ import annotations

import plistlib
import shutil
import subprocess
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version

_INFO_PLIST: dict[str, object] = {
    "CFBundleName": "Bani",
    "CFBundleDisplayName": "Bani",
    "CFBundleIdentifier": "dev.bani.app",
    "CFBundleVersion": "0.1.0",
    "CFBundleShortVersionString": "0.1.0",
    "CFBundleExecutable": "bani-launcher",
    "CFBundleIconFile": "AppIcon",
    "LSMinimumSystemVersion": "11.0",
    "LSUIElement": True,
    "NSHighResolutionCapable": True,
}


def build_app(arch: str = "aarch64") -> Path:
    """Build Bani.app as a self-contained macOS application.

    Args:
        arch: Target architecture (aarch64 or x86_64).

    Returns:
        Path to the built Bani.app bundle.
    """
    version = get_version()

    # Assemble the runtime (Python + Bani + UI)
    runtime_dir = assemble("darwin", arch)

    # Install menu bar dependencies into the runtime
    python_bin = runtime_dir / "python" / "bin" / "python3"
    print("\n=== Installing menu bar dependencies ===")
    subprocess.run(
        [
            str(python_bin),
            "-m",
            "pip",
            "install",
            "--no-cache-dir",
            "rumps>=0.4.0",
            "pyobjc-framework-Cocoa>=9.0",
        ],
        check=True,
    )

    # Build the .app bundle
    arch_label = "arm64" if arch == "aarch64" else "x86_64"
    app_dir = REPO_ROOT / "build" / f"Bani-{version}-{arch_label}" / "Bani.app"
    if app_dir.exists():
        shutil.rmtree(app_dir)

    contents = app_dir / "Contents"
    macos = contents / "MacOS"
    resources = contents / "Resources"
    runtime = resources / "runtime"

    macos.mkdir(parents=True)
    resources.mkdir(parents=True)

    # Move assembled runtime into Resources/runtime/
    shutil.move(str(runtime_dir), str(runtime))

    # Info.plist
    plist = dict(_INFO_PLIST)
    plist["CFBundleVersion"] = version
    plist["CFBundleShortVersionString"] = version
    plist_path = contents / "Info.plist"
    with plist_path.open("wb") as f:
        plistlib.dump(plist, f)

    # Copy bootstrap script
    shutil.copy(
        REPO_ROOT / "pkg_build" / "bani-start.py",
        resources / "bani-start.py",
    )

    # Fix Python dylib install_name for @rpath resolution
    python_dir = runtime / "python"
    dylib = python_dir / "lib" / "libpython3.12.dylib"
    print("\nFixing Python dylib install_name...")
    subprocess.run(
        ["install_name_tool", "-id", "@rpath/libpython3.12.dylib", str(dylib)],
        check=True,
    )

    # Compile native launcher (embeds Python for GUI access)
    launcher_src = REPO_ROOT / "pkg_build" / "launcher.m"
    launcher_bin = macos / "bani-launcher"
    print("Compiling native launcher...")
    subprocess.run(
        [
            "cc",
            "-o",
            str(launcher_bin),
            str(launcher_src),
            "-framework",
            "Cocoa",
            f"-I{python_dir}/include/python3.12",
            f"-L{python_dir}/lib",
            "-lpython3.12",
            "-Wl,-rpath,@executable_path/../Resources/runtime/python/lib",
        ],
        check=True,
    )

    print(f"\n=== Built: {app_dir} ===")
    return app_dir


def build_dmg(app_dir: Path) -> Path:
    """Wrap Bani.app in a .dmg disk image.

    Creates a DMG with the app and a symlink to /Applications
    for drag-to-install.

    Args:
        app_dir: Path to the Bani.app bundle.

    Returns:
        Path to the built .dmg file.
    """
    version = get_version()
    arch_label = "arm64" if "arm64" in str(app_dir) else "x86_64"
    dmg_name = f"Bani-{version}-macos-{arch_label}"

    # Create a staging directory with the app + Applications symlink
    staging = REPO_ROOT / "build" / f"{dmg_name}-dmg"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    shutil.copytree(str(app_dir), str(staging / "Bani.app"))
    (staging / "Applications").symlink_to("/Applications")

    # Build DMG
    dmg_path = REPO_ROOT / "build" / f"{dmg_name}.dmg"
    dmg_path.unlink(missing_ok=True)

    print(f"\n=== Building {dmg_path.name} ===")
    subprocess.run(
        [
            "hdiutil",
            "create",
            "-volname",
            "Bani",
            "-srcfolder",
            str(staging),
            "-ov",
            "-format",
            "UDZO",
            str(dmg_path),
        ],
        check=True,
    )

    # Clean up staging
    shutil.rmtree(staging)

    print(f"Built: {dmg_path}")
    return dmg_path


def build_pkg(arch: str = "aarch64") -> Path:
    """Build a flat .pkg installer for macOS.

    Installs Bani to /opt/bani with a symlink at /usr/local/bin/bani,
    matching the Linux package layout.

    Args:
        arch: Target architecture (aarch64 or x86_64).

    Returns:
        Path to the built .pkg file.
    """
    version = get_version()
    arch_label = "arm64" if arch == "aarch64" else "x86_64"

    # Assemble runtime (Python + Bani + UI)
    runtime_dir = assemble("darwin", arch)

    # Create pkg payload directory
    pkg_root = REPO_ROOT / "build" / "pkg-root"
    if pkg_root.exists():
        shutil.rmtree(pkg_root)

    opt_bani = pkg_root / "opt" / "bani"
    shutil.copytree(runtime_dir, opt_bani)

    # Symlink in /usr/local/bin
    usr_local_bin = pkg_root / "usr" / "local" / "bin"
    usr_local_bin.mkdir(parents=True)
    (usr_local_bin / "bani").symlink_to("/opt/bani/bin/bani")

    # Build .pkg
    output = REPO_ROOT / "build" / f"bani-{version}-macos-{arch_label}.pkg"
    output.unlink(missing_ok=True)

    print(f"\n=== Building {output.name} ===")
    subprocess.run(
        [
            "pkgbuild",
            "--root",
            str(pkg_root),
            "--identifier",
            "dev.bani.pkg",
            "--version",
            version,
            str(output),
        ],
        check=True,
    )

    # Clean up
    shutil.rmtree(pkg_root)
    shutil.rmtree(runtime_dir)

    print(f"Built: {output}")
    return output


if __name__ == "__main__":
    build_pkg()
