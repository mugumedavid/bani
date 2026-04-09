"""Bani macOS app bootstrap.

Launched by the native launcher binary. Sets up the NSApplication
status bar menu and starts the Bani web server in a background thread.
"""

import os
import secrets
import subprocess
import threading
import webbrowser

from AppKit import (
    NSApp,
    NSApplication,
    NSApplicationActivationPolicyAccessory,
    NSMenu,
    NSMenuItem,
    NSObject,
    NSStatusBar,
    NSVariableStatusItemLength,
)
from PyObjCTools import AppHelper

TOKEN = secrets.token_urlsafe(32)
PORT = 8910


def _runtime_bin() -> str:
    """Return the path to the runtime/bin directory."""
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "runtime", "bin")


class AppDelegate(NSObject):
    """NSApplication delegate that manages the status bar menu."""

    statusItem = None

    def applicationDidFinishLaunching_(self, notification):
        self.statusItem = (
            NSStatusBar.systemStatusBar()
            .statusItemWithLength_(NSVariableStatusItemLength)
        )
        self.statusItem.setTitle_("B")
        self.statusItem.setHighlightMode_(True)

        menu = NSMenu.alloc().init()

        items = [
            ("Open Bani", "openBrowser:"),
            ("Copy Token", "copyToken:"),
            ("Open Terminal", "openTerminal:"),
            None,
            (f"Server: localhost:{PORT}", None),
            None,
            ("Quit Bani", "quitApp:"),
        ]

        for item in items:
            if item is None:
                menu.addItem_(NSMenuItem.separatorItem())
                continue
            title, action = item
            mi = NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
                title, action, ""
            )
            if action:
                mi.setTarget_(self)
            else:
                mi.setEnabled_(False)
            menu.addItem_(mi)

        self.statusItem.setMenu_(menu)

        # Start server
        t = threading.Thread(target=_start_server, daemon=True)
        t.start()

        # Open browser after delay
        threading.Timer(2.0, self.openBrowser_, args=[None]).start()

    def openBrowser_(self, sender):
        webbrowser.open(f"http://localhost:{PORT}?token={TOKEN}")

    def copyToken_(self, sender):
        subprocess.run(["pbcopy"], input=TOKEN.encode(), check=False)

    def openTerminal_(self, sender):
        bin_dir = _runtime_bin()
        subprocess.run(
            [
                "osascript",
                "-e", 'tell application "Terminal"',
                "-e", "activate",
                "-e", (
                    f"do script \"export PATH='{bin_dir}':$PATH"
                    " && echo 'bani ready — try: bani --help'\""
                ),
                "-e", "end tell",
            ],
            check=False,
        )

    def quitApp_(self, sender):
        NSApp.terminate_(self)


def _start_server():
    os.environ["BANI_AUTH_TOKEN"] = TOKEN
    from bani.ui.server import BaniUIServer

    server = BaniUIServer(host="127.0.0.1", port=PORT)
    server.auth_token = TOKEN
    try:
        server.start()
    except Exception:
        pass  # Don't crash if server fails


app = NSApplication.sharedApplication()
app.setActivationPolicy_(NSApplicationActivationPolicyAccessory)
delegate = AppDelegate.alloc().init()
app.setDelegate_(delegate)
AppHelper.runEventLoop()
