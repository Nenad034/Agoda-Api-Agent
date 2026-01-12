"""MCP tools."""

import logging

from fastmcp import FastMCP

from .execute import register_execute_tool
from .query import register_query_tool

logger = logging.getLogger(__name__)


def register_all_tools(mcp: FastMCP) -> None:
    """Register all tools with generic internal names.

    Internal names (_query, _execute) are transformed by middleware
    to session-specific names (e.g., flights_query, catalog_execute).
    """
    register_query_tool(mcp)
    register_execute_tool(mcp)

    logger.info("Registered tools: _query, _execute (dynamically named per session)")
