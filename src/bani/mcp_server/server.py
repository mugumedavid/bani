"""Minimal MCP-compatible JSON-RPC 2.0 server over stdio (Section 18.6).

Implements the three MCP methods required for tool discovery and invocation:

* ``initialize``  — handshake, returns server info and capabilities.
* ``tools/list``   — returns all registered tool definitions.
* ``tools/call``   — dispatches a tool call to the appropriate handler.

The protocol is newline-delimited JSON over stdin/stdout.  No external MCP
SDK is used — the JSON-RPC surface is small enough to implement directly.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
from collections.abc import Callable
from typing import Any

from bani.mcp_server.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
    ToolHandler,
)

logger = logging.getLogger(__name__)

_SERVER_NAME = "bani"
_SERVER_VERSION = "0.1.0"


class McpServer:
    """JSON-RPC 2.0 server implementing the MCP tool protocol over stdio."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolHandler] = {}
        self._stdout_lock = threading.Lock()
        self._register_tools()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _register_tools(self) -> None:
        """Register all Bani tool handlers."""
        self._tools.update(TOOL_HANDLERS)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_stdio(self) -> None:
        """Read JSON-RPC requests from stdin, write responses to stdout.

        Each request and response is a single JSON object on one line
        (newline-delimited JSON).  The loop exits when stdin is closed.
        """
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                request: dict[str, Any] = json.loads(line)
            except json.JSONDecodeError as exc:
                self._write_response(
                    _error_response_no_id(-32700, f"Parse error: {exc}")
                )
                continue

            result = self._handle_request(request)
            if result is not None:
                self._write_response(result)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _handle_request(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """Route a JSON-RPC request to the appropriate handler.

        Returns ``None`` for JSON-RPC *notifications* (requests without an
        ``id``), which must not produce a response per the spec.
        """
        method = request.get("method", "")

        # JSON-RPC notifications have no "id" — never send a response.
        if "id" not in request:
            return None

        if method == "initialize":
            return self._handle_initialize(request)
        elif method == "tools/list":
            return self._handle_tools_list(request)
        elif method == "tools/call":
            return self._handle_tools_call(request)
        else:
            return _error_response(request, -32601, f"Method not found: {method}")

    # ------------------------------------------------------------------
    # MCP method handlers
    # ------------------------------------------------------------------

    def _handle_initialize(self, request: dict[str, Any]) -> dict[str, Any]:
        """Handle the ``initialize`` handshake."""
        return _success_response(
            request,
            {
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": _SERVER_NAME,
                    "version": _SERVER_VERSION,
                },
                "capabilities": {
                    "tools": {},
                },
            },
        )

    def _handle_tools_list(self, request: dict[str, Any]) -> dict[str, Any]:
        """Handle ``tools/list`` — return all tool definitions."""
        tools_list: list[dict[str, Any]] = []
        for td in TOOL_DEFINITIONS:
            tools_list.append(
                {
                    "name": td.name,
                    "description": td.description,
                    "inputSchema": td.input_schema,
                }
            )
        return _success_response(request, {"tools": tools_list})

    def _handle_tools_call(self, request: dict[str, Any]) -> dict[str, Any]:
        """Handle ``tools/call`` — dispatch to a tool handler."""
        params = request.get("params", {})
        tool_name = params.get("name", "")
        tool_args: dict[str, Any] = params.get("arguments", {})

        handler = self._tools.get(tool_name)
        if handler is None:
            return _error_response(
                request,
                -32602,
                f"Unknown tool: {tool_name}",
            )

        # Extract progressToken from _meta if present (MCP spec).
        meta = params.get("_meta") or {}
        progress_token = meta.get("progressToken")
        if progress_token is not None:
            tool_args["_progress_notifier"] = self._make_progress_notifier(
                progress_token
            )

        try:
            result = handler(tool_args)
            return _success_response(
                request,
                {
                    "content": result.content,
                    "isError": result.is_error,
                },
            )
        except Exception as exc:
            logger.exception("Tool '%s' raised an unhandled exception", tool_name)
            return _success_response(
                request,
                {
                    "content": [{"type": "text", "text": f"Internal error: {exc}"}],
                    "isError": True,
                },
            )

    # ------------------------------------------------------------------
    # Progress notifications
    # ------------------------------------------------------------------

    def _write_response(self, message: dict[str, Any]) -> None:
        """Write a JSON-RPC message to stdout (thread-safe)."""
        with self._stdout_lock:
            sys.stdout.write(json.dumps(message) + "\n")
            sys.stdout.flush()

    def _write_notification(self, notification: dict[str, Any]) -> None:
        """Write a JSON-RPC notification to stdout (thread-safe)."""
        self._write_response(notification)

    def _make_progress_notifier(
        self, token: str | int
    ) -> Callable[[int, int | None, str], None]:
        """Create a closure that emits ``notifications/progress``.

        Args:
            token: The ``progressToken`` from the client request.

        Returns:
            A callable ``(progress, total, message) -> None``.
        """

        def notify(progress: int, total: int | None, message: str) -> None:
            payload: dict[str, Any] = {
                "progressToken": token,
                "progress": progress,
                "message": message,
            }
            if total is not None:
                payload["total"] = total
            self._write_notification(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/progress",
                    "params": payload,
                }
            )

        return notify


# ---------------------------------------------------------------------------
# JSON-RPC helpers
# ---------------------------------------------------------------------------


def _success_response(request: dict[str, Any], result: Any) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "result": result,
    }


def _error_response(request: dict[str, Any], code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "error": {"code": code, "message": message},
    }


def _error_response_no_id(code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": code, "message": message},
    }
