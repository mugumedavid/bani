#!/usr/bin/env python3
"""Build a Windows .exe installer for Bani via Inno Setup.

Usage:
    python pkg_build/build_windows.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version


def build_exe(arch: str = "x86_64") -> Path:
    """Build a Windows .exe installer using Inno Setup.

    Args:
        arch: Target architecture (x86_64).

    Returns:
        Path to the built .exe file.
    """
    version = get_version()

    # Assemble the installation
    install_dir = assemble("windows", arch)

    # Install tray app dependencies (pystray + Pillow)
    python_exe = install_dir / "python" / "python.exe"
    subprocess.run(
        [
            str(python_exe),
            "-m",
            "pip",
            "install",
            "--no-cache-dir",
            "pystray>=0.19",
            "Pillow>=10.0",
        ],
        check=True,
    )

    # Generate Inno Setup script
    iss_path = REPO_ROOT / "build" / "bani.iss"
    output_dir = REPO_ROOT / "build"

    iss_path.write_text(
        f"[Setup]\n"
        f"AppName=Bani\n"
        f"AppVersion={version}\n"
        f"AppPublisher=David Mugume\n"
        f"AppPublisherURL=https://bani.dev\n"
        f"DefaultDirName={{autopf}}\\Bani\n"
        f"DefaultGroupName=Bani\n"
        f"OutputDir={output_dir}\n"
        f"OutputBaseFilename=bani-{version}-windows-x86_64-setup\n"
        f"Compression=lzma2\n"
        f"SolidCompression=yes\n"
        f"ArchitecturesAllowed=x64compatible\n"
        f"ArchitecturesInstallIn64BitMode=x64compatible\n"
        f"ChangesEnvironment=yes\n"
        f"\n"
        f"[Files]\n"
        f'Source: "{install_dir}\\*"; DestDir: "{{app}}"; Flags: recursesubdirs\n'
        f"\n"
        f"[Icons]\n"
        f'Name: "{{group}}\\Bani"; '
        f'Filename: "{{app}}\\bin\\bani-ui.vbs"; '
        f'Comment: "Launch Bani Web UI"\n'
        f'Name: "{{group}}\\Bani CLI"; '
        f'Filename: "cmd.exe"; '
        f'Parameters: "/k ""{{app}}\\bin\\bani.bat"" --help"\n'
        f'Name: "{{group}}\\Uninstall Bani"; '
        f'Filename: "{{uninstallexe}}"\n'
        f'Name: "{{commondesktop}}\\Bani"; '
        f'Filename: "{{app}}\\bin\\bani-ui.vbs"; '
        f'Comment: "Launch Bani Web UI"\n'
        f"\n"
        f"[Registry]\n"
        f"Root: HKLM; "
        f'Subkey: "SYSTEM\\CurrentControlSet\\Control'
        f'\\Session Manager\\Environment"; '
        f'ValueType: expandsz; ValueName: "Path"; '
        f'ValueData: "{{olddata}};{{app}}\\bin"; '
        f"Flags: preservestringtype\n"
        f"\n"
        f"[Run]\n"
        f'Filename: "wscript.exe"; '
        f'Parameters: """{{app}}\\bin\\bani-ui.vbs"""; '
        f'Description: "Launch Bani Web UI"; '
        f"Flags: postinstall nowait\n"
    )

    # Run Inno Setup compiler
    print("\nCompiling installer with Inno Setup...")
    iscc = "iscc"  # Must be on PATH (CI installs via choco)
    subprocess.run([iscc, str(iss_path)], check=True)

    output = output_dir / f"bani-{version}-windows-x86_64-setup.exe"
    print(f"Built: {output}")
    return output


if __name__ == "__main__":
    build_exe()
