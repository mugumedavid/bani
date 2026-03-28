"""CLI command for the MCP server (Section 18.6).

Exposes ``bani mcp serve`` which starts the Bani MCP server over stdio
(or, in the future, SSE transport).
"""

from __future__ import annotations

import typer

app = typer.Typer(help="MCP server commands.")


@app.command("serve")
def serve(
    transport: str = typer.Option(
        "stdio",
        help="Transport protocol: 'stdio' or 'sse'.",
    ),
    port: int = typer.Option(
        8080,
        help="Port for SSE transport (ignored for stdio).",
    ),
) -> None:
    """Start Bani as an MCP server."""
    from bani.mcp_server.server import McpServer

    server = McpServer()

    if transport == "stdio":
        server.run_stdio()
    else:
        typer.echo(f"SSE transport on port {port} is not yet implemented.")
        raise typer.Exit(1)
