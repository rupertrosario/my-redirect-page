"""Entry point: `python -m cohesity_hardware_mcp` starts the MCP server over
stdio, for registration in a .mcp.json / claude_desktop_config.json."""

from .server import mcp


def main():
    mcp.run()


if __name__ == "__main__":
    main()
