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
                response = _error_response_no_id(
                    -32700, f"Parse error: {exc}"
                )
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                continue

            response = self._handle_request(request)
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _handle_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Route a JSON-RPC request to the appropriate handler."""
        method = request.get("method", "")

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


# ---------------------------------------------------------------------------
# JSON-RPC helpers
# ---------------------------------------------------------------------------


def _success_response(
    request: dict[str, Any], result: Any
) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "result": result,
    }


def _error_response(
    request: dict[str, Any], code: int, message: str
) -> dict[str, Any]:
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
