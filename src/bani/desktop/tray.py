"""Bani Windows/Linux system tray app.

Runs the Bani web UI server in a background thread and provides
a system tray icon with options to open the browser, copy the token,
and quit.

Usage:
    pythonw -m bani.desktop.tray
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import threading
import webbrowser
from pathlib import Path
from typing import Any

import pystray
from PIL import Image, ImageDraw


def _bani_home() -> Path:
    """Resolve the Bani installation root."""
    for parent in Path(__file__).resolve().parents:
        bin_dir = parent / "bin"
        if (bin_dir / "bani").exists() or (bin_dir / "bani.bat").exists():
            return parent
    return Path(__file__).resolve().parent


def _create_icon_image() -> Image.Image:
    """Create a simple 'B' tray icon."""
    img = Image.new("RGBA", (64, 64), (0, 128, 128, 255))
    draw = ImageDraw.Draw(img)
    draw.text((20, 12), "B", fill="white")
    return img


class BaniTray:
    """System tray app for Bani."""

    def __init__(self) -> None:
        self.port = 8910
        self.token = secrets.token_urlsafe(32)
        self.bani_home = _bani_home()
        self.server_thread: threading.Thread | None = None

        self.icon = pystray.Icon(
            "bani",
            icon=_create_icon_image(),
            title="Bani",
            menu=pystray.Menu(
                pystray.MenuItem("Open Bani", self.open_browser, default=True),
                pystray.MenuItem("Copy Token", self.copy_token),
                pystray.MenuItem(
                    "Open Terminal",
                    self.open_terminal,
                ),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem(
                    f"Server: localhost:{self.port}",
                    None,
                    enabled=False,
                ),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Quit Bani", self.quit_app),
            ),
        )

    def open_browser(self, icon: Any = None, item: Any = None) -> None:
        """Open the Bani UI in the default browser."""
        url = f"http://localhost:{self.port}?token={self.token}"
        webbrowser.open(url)

    def copy_token(self, icon: Any = None, item: Any = None) -> None:
        """Copy the auth token to clipboard."""
        if sys.platform == "win32":
            # Use clip.exe on Windows
            subprocess.run(
                ["clip"],
                input=self.token.encode(),
                check=False,
            )
        else:
            # Use xclip on Linux
            subprocess.run(
                ["xclip", "-selection", "clipboard"],
                input=self.token.encode(),
                check=False,
            )

    def open_terminal(self, icon: Any = None, item: Any = None) -> None:
        """Open a terminal with bani on PATH."""
        bin_dir = self.bani_home / "bin"
        if sys.platform == "win32":
            # Open cmd.exe with bani on PATH
            cmd = f'set "PATH={bin_dir};%PATH%" && echo bani ready - try: bani --help'
            subprocess.Popen(
                ["cmd", "/k", cmd],
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
        else:
            # Linux: use .command-style approach or xterm
            import tempfile

            script = tempfile.NamedTemporaryFile(
                suffix=".sh",
                prefix="bani-",
                delete=False,
                mode="w",
            )
            script.write(
                "#!/bin/sh\n"
                f"export PATH='{bin_dir}':$PATH\n"
                "echo 'bani ready - try: bani --help'\n"
                'exec "$SHELL" -i\n'
            )
            script.close()
            os.chmod(script.name, 0o755)
            subprocess.Popen(["x-terminal-emulator", "-e", script.name])

    def quit_app(self, icon: Any = None, item: Any = None) -> None:
        """Stop the server and quit."""
        self.icon.stop()

    def _start_server(self) -> None:
        """Start the Bani UI server in a background thread."""
        import time as _time

        os.environ["BANI_AUTH_TOKEN"] = self.token

        from bani.ui.server import BaniUIServer

        for attempt in range(3):
            try:
                server = BaniUIServer(
                    host="127.0.0.1",
                    port=self.port,
                )
                server.auth_token = self.token
                server.start()
                break
            except Exception:
                if attempt < 2:
                    _time.sleep(2)
                    self.port += 1

    def run(self) -> None:
        """Start server then run the tray app."""
        self.server_thread = threading.Thread(
            target=self._start_server,
            daemon=True,
        )
        self.server_thread.start()

        # Open browser after a short delay
        threading.Timer(2.0, self.open_browser).start()

        self.icon.run()


def main() -> None:
    """Entry point."""
    BaniTray().run()


if __name__ == "__main__":
    main()
