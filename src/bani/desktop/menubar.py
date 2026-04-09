"""Bani macOS menu bar app.

Runs the Bani web UI server in a background thread and provides
a menu bar icon with options to open the browser, view the token,
and quit.

Usage:
    python -m bani.desktop.menubar
"""

from __future__ import annotations

import secrets
import subprocess
import threading
import webbrowser
from typing import Any

import rumps


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
        self.server_thread: threading.Thread | None = None
        self.server_running = False

        self.menu = [
            rumps.MenuItem("Open Bani", callback=self.open_browser),
            rumps.MenuItem("Copy Token", callback=self.copy_token),
            None,  # separator
            rumps.MenuItem(
                f"Server: localhost:{self.port}",
                callback=None,
            ),
            None,  # separator
            rumps.MenuItem("Quit Bani", callback=self.quit_app),
        ]

    def open_browser(self, _: Any) -> None:
        """Open the Bani UI in the default browser."""
        url = f"http://localhost:{self.port}?token={self.token}"
        webbrowser.open(url)

    def copy_token(self, _: Any) -> None:
        """Copy the auth token to clipboard."""
        subprocess.run(
            ["pbcopy"],
            input=self.token.encode(),
            check=False,
        )
        rumps.notification(
            "Bani",
            "Token copied",
            "Auth token copied to clipboard.",
        )

    def quit_app(self, _: Any) -> None:
        """Stop the server and quit."""
        rumps.quit_application()

    def _start_server(self) -> None:
        """Start the Bani UI server in a background thread."""
        import os

        os.environ["BANI_AUTH_TOKEN"] = self.token

        from bani.ui.server import BaniUIServer

        server = BaniUIServer(
            host="127.0.0.1",
            port=self.port,
        )
        # Override the token with ours
        server.auth_token = self.token
        self.server_running = True
        server.start()

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
