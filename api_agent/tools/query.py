"""Unified MCP tool for natural language API queries."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from ..agent.graphql_agent import process_query
from ..agent.rest_agent import process_rest_query
from ..context import MissingHeaderError, get_request_context


def register_query_tool(mcp: FastMCP) -> None:
    """Register the unified query tool with generic internal name."""

    @mcp.tool(
        name="_query",
        description="""Ask questions about the API in natural language.

The agent reads the schema, builds queries, executes them, and can do multi-step data processing.

Returns answer and the queries/calls made (reusable with execute tool).""",
        tags={"query", "nl"},
    )
    async def query(
        question: Annotated[str, Field(description="Natural language question about the API")],
    ) -> dict:
        """Process natural language query against configured API."""
        try:
            ctx = get_request_context()
        except MissingHeaderError as e:
            return {"ok": False, "error": str(e)}

        if ctx.api_type == "graphql":
            result = await process_query(question, ctx)
            response = {
                "ok": result.get("ok", False),
                "data": result.get("data"),
                "queries": result.get("queries", []),
                "error": result.get("error"),
            }
            if ctx.include_result:
                response["result"] = result.get("result")
            return response
        else:
            result = await process_rest_query(question, ctx)
            response = {
                "ok": result.get("ok", False),
                "data": result.get("data"),
                "api_calls": result.get("api_calls", []),
                "error": result.get("error"),
            }
            if ctx.include_result:
                response["result"] = result.get("result")
            return response
