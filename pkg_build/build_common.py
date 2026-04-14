#!/usr/bin/env python3
"""Shared build logic for cross-platform Bani packaging.

Assembles a self-contained Bani installation:
1. Fetches python-build-standalone for the target platform
2. Installs Bani + all dependencies into the bundled env
3. Copies the pre-built React UI
4. Creates wrapper scripts (bani / bani.bat)

The resulting directory tree is ready to be wrapped by a
platform-specific installer (build_*.py).
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

# Repo root (assumes this script is at pkg_build/build_common.py)
REPO_ROOT = Path(__file__).resolve().parent.parent

# Bani version (read from pyproject.toml)
VERSION = "0.1.0"


def get_version() -> str:
    """Read version from pyproject.toml."""
    toml_path = REPO_ROOT / "pyproject.toml"
    for line in toml_path.read_text().splitlines():
        if line.strip().startswith("version"):
            return line.split("=")[1].strip().strip('"')
    return VERSION


def build_ui() -> Path:
    """Build the React UI if not already built.

    Returns:
        Path to the ui/dist directory.
    """
    dist_dir = REPO_ROOT / "ui" / "dist"
    if dist_dir.exists() and (dist_dir / "index.html").exists():
        print("UI already built")
        return dist_dir

    print("Building React UI...")
    subprocess.run(
        ["npm", "run", "build"],
        cwd=REPO_ROOT / "ui",
        check=True,
    )
    return dist_dir


def assemble(
    target_os: str,
    target_arch: str,
    output_dir: Path | None = None,
) -> Path:
    """Assemble a complete Bani installation directory.

    Args:
        target_os: Target OS (linux, darwin, windows).
        target_arch: Target architecture (x86_64, aarch64).
        output_dir: Where to create the installation tree.
            Defaults to ``build/bani-{version}-{os}-{arch}/``.

    Returns:
        Path to the assembled directory.
    """
    from pkg_build.fetch_python import fetch

    version = get_version()
    if output_dir is None:
        output_dir = REPO_ROOT / "build" / f"bani-{version}-{target_os}-{target_arch}"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    # 1. Fetch standalone Python
    print(f"\n=== Fetching Python for {target_os}/{target_arch} ===")
    python_src = fetch(target_os, target_arch)

    python_dest = output_dir / "python"
    print(f"Copying Python to {python_dest}...")
    shutil.copytree(python_src, python_dest)

    # Determine Python executable path
    if target_os == "windows":
        python_exe = python_dest / "python.exe"
    else:
        python_exe = python_dest / "bin" / "python3"

    # Bootstrap pip (stripped distributions may not have pip entry-point scripts)
    subprocess.run(
        [str(python_exe), "-m", "ensurepip", "--upgrade"],
        check=True,
    )

    # 2. Install Bani + dependencies
    print("\n=== Installing Bani + dependencies ===")
    wheel_dir = REPO_ROOT / "dist"
    if not list(wheel_dir.glob("bani-*.whl")):
        print("Building wheel first...")
        subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir)],
            cwd=REPO_ROOT,
            check=True,
        )

    wheel = next(wheel_dir.glob("bani-*.whl"))
    subprocess.run(
        [str(python_exe), "-m", "pip", "install", "--no-cache-dir", str(wheel)],
        check=True,
    )

    # 3. Copy pre-built UI
    print("\n=== Copying UI ===")
    ui_dist = build_ui()
    ui_dest = output_dir / "ui" / "dist"
    shutil.copytree(ui_dist, ui_dest)

    # Also copy into the Python package so the server finds it
    site_packages = _find_site_packages(python_dest, target_os)
    if site_packages:
        bani_ui_dist = site_packages / "bani" / "ui" / "dist"
        if bani_ui_dist.parent.exists():
            if bani_ui_dist.exists():
                shutil.rmtree(bani_ui_dist)
            shutil.copytree(ui_dist, bani_ui_dist)

    # 4. Create wrapper scripts
    print("\n=== Creating wrapper scripts ===")
    bin_dir = output_dir / "bin"
    bin_dir.mkdir(exist_ok=True)

    if target_os == "windows":
        _create_windows_wrapper(bin_dir, output_dir)
    else:
        _create_unix_wrapper(bin_dir, output_dir)

    print(f"\n=== Assembly complete: {output_dir} ===")
    return output_dir


def _find_site_packages(python_dir: Path, target_os: str) -> Path | None:
    """Find the site-packages directory inside the Python install."""
    if target_os == "windows":
        sp = python_dir / "Lib" / "site-packages"
    else:
        # Look for lib/python3.*/site-packages
        lib_dir = python_dir / "lib"
        if lib_dir.exists():
            for d in lib_dir.iterdir():
                if d.name.startswith("python3"):
                    sp = d / "site-packages"
                    if sp.exists():
                        return sp
        sp = python_dir / "lib" / "python3.12" / "site-packages"
    return sp if sp.exists() else None


def _create_unix_wrapper(bin_dir: Path, install_dir: Path) -> None:
    """Create a Unix shell wrapper script."""
    wrapper = bin_dir / "bani"
    wrapper.write_text(
        "#!/bin/sh\n"
        'BANI_HOME="$(cd "$(dirname "$0")/.." && pwd)"\n'
        'exec "$BANI_HOME/python/bin/python3" -m bani "$@"\n'
    )
    wrapper.chmod(0o755)
    print(f"Created {wrapper}")


def _create_windows_wrapper(bin_dir: Path, install_dir: Path) -> None:
    """Create Windows batch wrapper and hidden UI launcher."""
    wrapper = bin_dir / "bani.bat"
    wrapper.write_text(
        "@echo off\r\n"
        'set "BANI_HOME=%~dp0.."\r\n'
        '"%BANI_HOME%\\python\\python.exe" -m bani %*\r\n'
    )
    print(f"Created {wrapper}")

    # Hidden tray launcher — runs the system tray app without a console.
    # Uses pythonw.exe (windowless Python) so no cmd window appears.
    ui_launcher = bin_dir / "bani-ui.vbs"
    ui_launcher.write_text(
        'Set WshShell = CreateObject("WScript.Shell")\r\n'
        'BaniHome = CreateObject("Scripting.FileSystemObject")'
        ".GetParentFolderName(WScript.ScriptFullName)\r\n"
        'PythonW = BaniHome & "\\..\\python\\pythonw.exe"\r\n'
        "WshShell.Run Chr(34) & PythonW & Chr(34) "
        '& " -m bani.desktop.tray", 0, False\r\n'
    )
    print(f"Created {ui_launcher}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Assemble Bani installation")
    parser.add_argument("--os", dest="target_os", help="Target OS")
    parser.add_argument("--arch", dest="target_arch", help="Target arch")
    parser.add_argument("--output", help="Output directory")
    args = parser.parse_args()

    if args.target_os and args.target_arch:
        target_os, target_arch = args.target_os, args.target_arch
    else:
        from pkg_build.fetch_python import detect_platform

        target_os, target_arch = detect_platform()

    output = Path(args.output) if args.output else None
    assemble(target_os, target_arch, output)


if __name__ == "__main__":
    main()
