#!/usr/bin/env python3
"""Download python-build-standalone for the target platform.

Downloads a self-contained Python 3.12 distribution from
https://github.com/indygreg/python-build-standalone and extracts it
to ``build/python-standalone/``.

Usage:
    python pkg_build/fetch_python.py                  # auto-detect
    python pkg_build/fetch_python.py --os linux --arch x86_64
    python pkg_build/fetch_python.py --os windows --arch x86_64
"""

from __future__ import annotations

import argparse
import io
import platform
import shutil
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path

# Python version to bundle
PYTHON_VERSION = "3.12.8"

# python-build-standalone release tag
RELEASE_TAG = "20241219"

# Base URL for releases
BASE_URL = (
    f"https://github.com/indygreg/python-build-standalone/releases/download/"
    f"{RELEASE_TAG}"
)

# Mapping of (os, arch) to the release filename
_PREFIX = f"cpython-{PYTHON_VERSION}+{RELEASE_TAG}"
_SUFFIX = "install_only_stripped.tar.gz"
TARGETS: dict[tuple[str, str], str] = {
    ("linux", "x86_64"): f"{_PREFIX}-x86_64-unknown-linux-gnu-{_SUFFIX}",
    ("linux", "aarch64"): f"{_PREFIX}-aarch64-unknown-linux-gnu-{_SUFFIX}",
    ("darwin", "x86_64"): f"{_PREFIX}-x86_64-apple-darwin-{_SUFFIX}",
    ("darwin", "aarch64"): f"{_PREFIX}-aarch64-apple-darwin-{_SUFFIX}",
    ("windows", "x86_64"): f"{_PREFIX}-x86_64-pc-windows-msvc-{_SUFFIX}",
}


def detect_platform() -> tuple[str, str]:
    """Detect current OS and architecture."""
    os_name = sys.platform
    if os_name.startswith("linux"):
        os_name = "linux"
    elif os_name == "darwin":
        os_name = "darwin"
    elif os_name == "win32":
        os_name = "windows"

    arch = platform.machine().lower()
    if arch in ("arm64", "aarch64"):
        arch = "aarch64"
    elif arch in ("x86_64", "amd64"):
        arch = "x86_64"

    return os_name, arch


def fetch(
    target_os: str,
    target_arch: str,
    output_dir: Path = Path("build/python-standalone"),
) -> Path:
    """Download and extract python-build-standalone.

    Args:
        target_os: Target OS (linux, darwin, windows).
        target_arch: Target architecture (x86_64, aarch64).
        output_dir: Directory to extract into.

    Returns:
        Path to the extracted Python installation.
    """
    key = (target_os, target_arch)
    if key not in TARGETS:
        available = ", ".join(f"{o}/{a}" for o, a in TARGETS)
        print(f"Error: unsupported target {target_os}/{target_arch}")
        print(f"Available: {available}")
        sys.exit(1)

    filename = TARGETS[key]
    url = f"{BASE_URL}/{filename}"

    # Check if already extracted
    python_dir = output_dir / "python"
    if python_dir.exists():
        print(f"Python already extracted at {python_dir}")
        return python_dir

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {filename}...")
    print(f"  URL: {url}")

    response = urllib.request.urlopen(url)
    data = response.read()
    print(f"  Downloaded {len(data) / 1024 / 1024:.1f} MB")

    # Extract
    print("Extracting...")
    if filename.endswith(".tar.gz"):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(output_dir)
    elif filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(output_dir)

    # python-build-standalone extracts to python/install/ — normalize
    install_dir = output_dir / "python" / "install"
    if install_dir.exists():
        # Move install/* up to python/
        for item in install_dir.iterdir():
            shutil.move(str(item), str(output_dir / "python" / item.name))
        install_dir.rmdir()

    print(f"Python extracted to {python_dir}")
    return python_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch python-build-standalone",
    )
    parser.add_argument("--os", dest="target_os", help="Target OS")
    parser.add_argument("--arch", dest="target_arch", help="Target arch")
    parser.add_argument(
        "--output",
        default="build/python-standalone",
        help="Output dir",
    )
    args = parser.parse_args()

    if args.target_os and args.target_arch:
        target_os, target_arch = args.target_os, args.target_arch
    else:
        target_os, target_arch = detect_platform()
        print(f"Detected platform: {target_os}/{target_arch}")

    fetch(target_os, target_arch, Path(args.output))


if __name__ == "__main__":
    main()
