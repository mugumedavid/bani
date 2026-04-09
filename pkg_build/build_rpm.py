#!/usr/bin/env python3
"""Build an RPM package for Bani (RHEL/Fedora).

Usage:
    python pkg_build/build_rpm.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version


def build_rpm(arch: str = "x86_64") -> Path:
    """Build an .rpm package.

    Args:
        arch: Target architecture (x86_64 or aarch64).

    Returns:
        Path to the built .rpm file.
    """
    version = get_version()
    rpm_arch = arch if arch == "x86_64" else "aarch64"

    # Assemble the installation
    install_dir = assemble("linux", arch)

    # Set up rpmbuild directory structure
    rpmbuild_dir = REPO_ROOT / "build" / "rpmbuild"
    for d in ["BUILD", "RPMS", "SOURCES", "SPECS", "SRPMS", "BUILDROOT"]:
        (rpmbuild_dir / d).mkdir(parents=True, exist_ok=True)

    import shutil

    # Stage assembled files in SOURCES so %install can copy them
    # into BUILDROOT (rpmbuild wipes BUILDROOT before %install).
    staged = rpmbuild_dir / "SOURCES" / "bani-install"
    if staged.exists():
        shutil.rmtree(staged)
    shutil.copytree(install_dir, staged)

    # Create .spec file
    spec = rpmbuild_dir / "SPECS" / "bani.spec"
    spec.write_text(
        f"Name: bani\n"
        f"Version: {version}\n"
        f"Release: 1\n"
        f"Summary: Database migration engine powered by Apache Arrow\n"
        f"License: Apache-2.0\n"
        f"URL: https://bani.dev\n"
        f"BuildArch: {rpm_arch}\n"
        f"\n"
        f"%description\n"
        f"Bani migrates data between PostgreSQL, MySQL, MSSQL, Oracle,\n"
        f"and SQLite with a Web UI, CLI, SDK, and MCP server.\n"
        f"\n"
        f"%install\n"
        f"mkdir -p %{{buildroot}}/opt\n"
        f"cp -a %{{_topdir}}/SOURCES/bani-install %{{buildroot}}/opt/bani\n"
        f"mkdir -p %{{buildroot}}/usr/local/bin\n"
        f"ln -sf /opt/bani/bin/bani %{{buildroot}}/usr/local/bin/bani\n"
        f"\n"
        f"%files\n"
        f"/opt/bani\n"
        f"/usr/local/bin/bani\n"
    )

    buildroot = rpmbuild_dir / "BUILDROOT" / f"bani-{version}-1.{rpm_arch}"

    # Build RPM
    print("\nBuilding RPM...")
    subprocess.run(
        [
            "rpmbuild",
            "-bb",
            "--define",
            f"_topdir {rpmbuild_dir}",
            "--buildroot",
            str(buildroot),
            str(spec),
        ],
        check=True,
    )

    # Find the output RPM
    rpm_dir = rpmbuild_dir / "RPMS" / rpm_arch
    rpms = list(rpm_dir.glob("bani-*.rpm"))
    if rpms:
        output = rpms[0]
        final = REPO_ROOT / "build" / output.name
        shutil.copy2(output, final)
        print(f"Built: {final}")
        return final

    print("RPM build completed but output not found")
    return rpm_dir


if __name__ == "__main__":
    build_rpm()
