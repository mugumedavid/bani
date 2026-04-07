"""Entry point for ``python -m bani.mcp_server``."""

from bani.mcp_server.server import McpServer


def main() -> None:
    McpServer().run_stdio()


if __name__ == "__main__":
    main()
