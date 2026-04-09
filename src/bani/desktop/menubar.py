"""Bani macOS menu bar app.

Runs the Bani web UI server in a background thread and provides
a menu bar icon with options to open the browser, open a terminal
with bani on PATH, view the token, and quit.

Usage:
    python -m bani.desktop.menubar
"""

from __future__ import annotations

import os
import secrets
import subprocess
import threading
import webbrowser
from pathlib import Path
from typing import Any

import rumps


def _bani_home() -> Path:
    """Resolve the Bani installation root.

    When running from Bani.app, the layout is:
        Bani.app/Contents/MacOS/bani-launcher
        Bani.app/Contents/Resources/runtime/bin/bani
        Bani.app/Contents/Resources/runtime/python/...
    """
    # Try app bundle layout first
    resources = Path(__file__).resolve().parent
    # Installed in site-packages inside the runtime
    # Walk up to find the runtime/bin directory
    for parent in resources.parents:
        bin_dir = parent / "bin"
        if (bin_dir / "bani").exists():
            return parent
    # Fallback: /opt/bani
    return Path("/opt/bani")


class BaniApp(rumps.App):
    """Menu bar app for Bani."""

    def __init__(self) -> None:
        super().__init__(
            "Bani",
            title="B",
            quit_button=None,
        )
        self.port = 8910
        self.token = secrets.token_urlsafe(32)
        self.bani_home = _bani_home()
        self.server_thread: threading.Thread | None = None

        self.menu = [
            rumps.MenuItem("Open Bani", callback=self.open_browser),
            rumps.MenuItem(
                "Open Terminal",
                callback=self.open_terminal,
            ),
            rumps.MenuItem("Copy Token", callback=self.copy_token),
            None,
            rumps.MenuItem(
                f"Server: localhost:{self.port}",
                callback=None,
            ),
            None,
            rumps.MenuItem("Quit Bani", callback=self.quit_app),
        ]

    def open_browser(self, _: Any) -> None:
        """Open the Bani UI in the default browser."""
        url = f"http://localhost:{self.port}?token={self.token}"
        webbrowser.open(url)

    def open_terminal(self, _: Any) -> None:
        """Open Terminal.app with bani on PATH."""
        bin_dir = self.bani_home / "bin"
        cmd = (
            f"export PATH='{bin_dir}':$PATH"
            " && echo 'bani ready — try: bani --help'"
        )
        subprocess.run(
            [
                "osascript",
                "-e",
                'tell application "Terminal"',
                "-e",
                "activate",
                "-e",
                f'do script "{cmd}"',
                "-e",
                "end tell",
            ],
            check=False,
        )

    def copy_token(self, _: Any) -> None:
        """Copy the auth token to clipboard."""
        subprocess.run(
            ["pbcopy"],
            input=self.token.encode(),
            check=False,
        )
        # Use osascript for notification — rumps.notification()
        # requires a PlistBuddy setup that may not be present.
        subprocess.run(
            [
                "osascript",
                "-e",
                "display notification "
                '"Auth token copied to clipboard." '
                'with title "Bani"',
            ],
            check=False,
        )

    def quit_app(self, _: Any) -> None:
        """Stop the server and quit."""
        rumps.quit_application()

    def _start_server(self) -> None:
        """Start the Bani UI server in a background thread.

        Retries on port conflict and catches all exceptions so the
        menu bar stays alive even if the server fails.
        """
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
                    self.port += 1  # try next port
                    self.title = f"B:{self.port}"

    def run(self, **kwargs: Any) -> None:
        """Start server then run the menu bar app."""
        self.server_thread = threading.Thread(
            target=self._start_server,
            daemon=True,
        )
        self.server_thread.start()

        # Open browser after a short delay
        threading.Timer(
            2.0,
            self.open_browser,
            args=[None],
        ).start()

        super().run(**kwargs)


def main() -> None:
    """Entry point."""
    BaniApp().run()


if __name__ == "__main__":
    main()
