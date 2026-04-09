#!/usr/bin/env python3
"""Build a Debian .deb package for Bani.

Usage:
    python pkg_build/build_deb.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version


def build_deb(arch: str = "x86_64") -> Path:
    """Build a .deb package.

    Args:
        arch: Target architecture (x86_64 or aarch64).

    Returns:
        Path to the built .deb file.
    """
    version = get_version()
    deb_arch = "amd64" if arch == "x86_64" else "arm64"

    # Assemble the installation
    install_dir = assemble("linux", arch)

    # Create .deb structure
    deb_root = REPO_ROOT / "build" / f"bani_{version}_{deb_arch}"
    if deb_root.exists():
        import shutil

        shutil.rmtree(deb_root)

    # /opt/bani/
    opt_bani = deb_root / "opt" / "bani"
    import shutil

    shutil.copytree(install_dir, opt_bani)

    # /usr/local/bin/bani symlink
    usr_bin = deb_root / "usr" / "local" / "bin"
    usr_bin.mkdir(parents=True)
    (usr_bin / "bani").symlink_to("/opt/bani/bin/bani")

    # DEBIAN/control
    debian_dir = deb_root / "DEBIAN"
    debian_dir.mkdir()
    (debian_dir / "control").write_text(
        f"Package: bani\n"
        f"Version: {version}\n"
        f"Section: database\n"
        f"Priority: optional\n"
        f"Architecture: {deb_arch}\n"
        f"Maintainer: David Mugume <mugumedavid@gmail.com>\n"
        f"Description: Database migration engine powered by Apache Arrow\n"
        f" Bani migrates data between PostgreSQL, MySQL, MSSQL, Oracle,\n"
        f" and SQLite with a Web UI, CLI, SDK, and MCP server.\n"
        f"Homepage: https://bani.dev\n"
    )

    # DEBIAN/postinst — make wrapper executable
    postinst = debian_dir / "postinst"
    postinst.write_text("#!/bin/sh\nchmod +x /opt/bani/bin/bani\n")
    postinst.chmod(0o755)

    # Build .deb
    output = REPO_ROOT / "build" / f"bani_{version}_{deb_arch}.deb"
    print(f"\nBuilding {output.name}...")
    subprocess.run(
        ["dpkg-deb", "--build", str(deb_root), str(output)],
        check=True,
    )
    print(f"Built: {output}")
    return output


if __name__ == "__main__":
    build_deb()
