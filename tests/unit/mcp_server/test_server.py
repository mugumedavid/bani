"""Tests for the MCP JSON-RPC server."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from bani.mcp_server.server import McpServer
from bani.mcp_server.tools import TOOL_DEFINITIONS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    method: str,
    params: dict | None = None,
    request_id: int = 1,
) -> dict:
    """Build a minimal JSON-RPC request dict."""
    req: dict = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        req["params"] = params
    return req


# ---------------------------------------------------------------------------
# Initialize
# ---------------------------------------------------------------------------


class TestInitialize:
    """Tests for the ``initialize`` method."""

    def test_returns_server_info(self) -> None:
        """Initialize response contains serverInfo with name and version."""
        server = McpServer()
        request = _make_request("initialize")
        response = server._handle_request(request)

        assert response["jsonrpc"] == "2.0"
        assert response["id"] == 1
        result = response["result"]
        assert result["serverInfo"]["name"] == "bani"
        assert result["serverInfo"]["version"] == "0.1.0"

    def test_returns_capabilities(self) -> None:
        """Initialize response declares tool capabilities."""
        server = McpServer()
        response = server._handle_request(_make_request("initialize"))
        assert "tools" in response["result"]["capabilities"]

    def test_returns_protocol_version(self) -> None:
        """Initialize response includes a protocolVersion."""
        server = McpServer()
        response = server._handle_request(_make_request("initialize"))
        assert "protocolVersion" in response["result"]


# ---------------------------------------------------------------------------
# tools/list
# ---------------------------------------------------------------------------


class TestToolsList:
    """Tests for ``tools/list``."""

    def test_returns_all_ten_tools(self) -> None:
        """The tools list must contain exactly 10 tool definitions."""
        server = McpServer()
        response = server._handle_request(_make_request("tools/list"))

        tools = response["result"]["tools"]
        assert len(tools) == 10

    def test_tool_names_match_definitions(self) -> None:
        """Returned tool names must match the global TOOL_DEFINITIONS."""
        server = McpServer()
        response = server._handle_request(_make_request("tools/list"))

        returned_names = {t["name"] for t in response["result"]["tools"]}
        expected_names = {td.name for td in TOOL_DEFINITIONS}
        assert returned_names == expected_names

    def test_each_tool_has_required_fields(self) -> None:
        """Each tool entry must have name, description, and inputSchema."""
        server = McpServer()
        response = server._handle_request(_make_request("tools/list"))

        for tool in response["result"]["tools"]:
            assert "name" in tool
            assert "description" in tool
            assert "inputSchema" in tool
            assert isinstance(tool["inputSchema"], dict)

    def test_expected_tool_names(self) -> None:
        """Verify the specific tool names we expect."""
        server = McpServer()
        response = server._handle_request(_make_request("tools/list"))

        names = {t["name"] for t in response["result"]["tools"]}
        expected = {
            "bani_connections",
            "bani_schema_inspect",
            "bani_validate_bdl",
            "bani_save_project",
            "bani_preview",
            "bani_run",
            "bani_status",
            "bani_connectors_list",
            "bani_connector_info",
            "bani_generate_bdl",
        }
        assert names == expected


# ---------------------------------------------------------------------------
# tools/call
# ---------------------------------------------------------------------------


class TestToolsCall:
    """Tests for ``tools/call`` dispatch."""

    def test_dispatches_to_handler(self) -> None:
        """tools/call dispatches to the correct handler and returns its result."""
        server = McpServer()

        # Use bani_connectors_list since it needs no external deps
        with patch(
            "bani.connectors.registry.ConnectorRegistry.discover",
            return_value={"postgresql": MagicMock()},
        ):
            response = server._handle_request(
                _make_request(
                    "tools/call",
                    params={"name": "bani_connectors_list", "arguments": {}},
                )
            )

        assert "result" in response
        assert response["result"]["isError"] is False
        content = response["result"]["content"]
        data = json.loads(content[0]["text"])
        assert "postgresql" in data["connectors"]

    def test_unknown_tool_returns_error(self) -> None:
        """Calling a nonexistent tool returns a JSON-RPC error."""
        server = McpServer()
        response = server._handle_request(
            _make_request(
                "tools/call",
                params={"name": "bani_nonexistent", "arguments": {}},
            )
        )

        assert "error" in response
        assert response["error"]["code"] == -32602
        assert "bani_nonexistent" in response["error"]["message"]

    def test_handler_exception_returns_is_error(self) -> None:
        """If a handler raises an unhandled exception, isError is True."""
        server = McpServer()
        server._tools["bani_explode"] = MagicMock(
            side_effect=RuntimeError("boom")
        )

        response = server._handle_request(
            _make_request(
                "tools/call",
                params={"name": "bani_explode", "arguments": {}},
            )
        )

        assert response["result"]["isError"] is True
        assert "boom" in response["result"]["content"][0]["text"]

    def test_passes_arguments_to_handler(self) -> None:
        """tool/call passes the 'arguments' dict as params to the handler."""
        server = McpServer()

        result = MagicMock()
        result.content = [{"type": "text", "text": "ok"}]
        result.is_error = False

        spy = MagicMock(return_value=result)
        server._tools["bani_generate_bdl"] = spy

        server._handle_request(
            _make_request(
                "tools/call",
                params={
                    "name": "bani_generate_bdl",
                    "arguments": {
                        "source_connector": "pg",
                        "target_connector": "mysql",
                    },
                },
            )
        )

        spy.assert_called_once_with(
            {"source_connector": "pg", "target_connector": "mysql"}
        )


# ---------------------------------------------------------------------------
# Unknown method
# ---------------------------------------------------------------------------


class TestUnknownMethod:
    """Tests for unknown JSON-RPC methods."""

    def test_returns_method_not_found(self) -> None:
        """Unknown methods get a -32601 error."""
        server = McpServer()
        response = server._handle_request(
            _make_request("some/unknown/method")
        )

        assert "error" in response
        assert response["error"]["code"] == -32601
        assert "not found" in response["error"]["message"].lower()


# ---------------------------------------------------------------------------
# Malformed requests
# ---------------------------------------------------------------------------


class TestMalformedRequests:
    """Tests for malformed or edge-case JSON-RPC requests."""

    def test_missing_method_field(self) -> None:
        """A request with no 'method' key is treated as method-not-found."""
        server = McpServer()
        response = server._handle_request({"jsonrpc": "2.0", "id": 1})

        assert "error" in response
        assert response["error"]["code"] == -32601

    def test_missing_id_field(self) -> None:
        """A request without 'id' is a JSON-RPC notification — no response."""
        server = McpServer()
        response = server._handle_request(
            {"jsonrpc": "2.0", "method": "initialize"}
        )

        assert response is None

    def test_tools_call_missing_params(self) -> None:
        """tools/call with no params still dispatches (with empty name)."""
        server = McpServer()
        response = server._handle_request(
            _make_request("tools/call")
        )

        # Should fail because empty tool name is unknown
        assert "error" in response
        assert response["error"]["code"] == -32602

    def test_response_id_matches_request(self) -> None:
        """The response id must match the request id."""
        server = McpServer()
        for rid in [1, 42, "abc"]:
            req = {"jsonrpc": "2.0", "id": rid, "method": "initialize"}
            resp = server._handle_request(req)
            assert resp["id"] == rid


# ---------------------------------------------------------------------------
# stdio integration (unit-level)
# ---------------------------------------------------------------------------


class TestRunStdio:
    """Verify run_stdio reads from stdin and writes to stdout."""

    def test_processes_single_request(self) -> None:
        """A single initialize request is processed and flushed."""
        server = McpServer()
        request_line = json.dumps(_make_request("initialize")) + "\n"

        with (
            patch("sys.stdin", [request_line]),
            patch("sys.stdout") as mock_stdout,
        ):
            server.run_stdio()

        mock_stdout.write.assert_called_once()
        written = mock_stdout.write.call_args[0][0]
        response = json.loads(written.strip())
        assert response["result"]["serverInfo"]["name"] == "bani"
        mock_stdout.flush.assert_called_once()

    def test_handles_invalid_json(self) -> None:
        """Invalid JSON produces a parse-error response."""
        server = McpServer()

        with (
            patch("sys.stdin", ["not valid json\n"]),
            patch("sys.stdout") as mock_stdout,
        ):
            server.run_stdio()

        written = mock_stdout.write.call_args[0][0]
        response = json.loads(written.strip())
        assert "error" in response
        assert response["error"]["code"] == -32700

    def test_skips_empty_lines(self) -> None:
        """Empty lines are skipped without error."""
        server = McpServer()
        request_line = json.dumps(_make_request("initialize")) + "\n"

        with (
            patch("sys.stdin", ["\n", "  \n", request_line]),
            patch("sys.stdout") as mock_stdout,
        ):
            server.run_stdio()

        # Only one response should be written (for the initialize request)
        assert mock_stdout.write.call_count == 1

    def test_multiple_requests(self) -> None:
        """Multiple requests are processed sequentially."""
        server = McpServer()
        lines = [
            json.dumps(_make_request("initialize", request_id=1)) + "\n",
            json.dumps(_make_request("tools/list", request_id=2)) + "\n",
        ]

        with (
            patch("sys.stdin", lines),
            patch("sys.stdout") as mock_stdout,
        ):
            server.run_stdio()

        assert mock_stdout.write.call_count == 2
        assert mock_stdout.flush.call_count == 2

        # Check first response
        resp1 = json.loads(mock_stdout.write.call_args_list[0][0][0].strip())
        assert resp1["id"] == 1

        # Check second response
        resp2 = json.loads(mock_stdout.write.call_args_list[1][0][0].strip())
        assert resp2["id"] == 2
