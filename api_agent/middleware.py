"""FastMCP middleware for dynamic tool naming per session."""

import re
from collections.abc import Sequence

from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import Tool as FastMCPTool
from mcp import types as mt

from .context import extract_api_name, get_full_hostname

# Internal tool name suffix pattern
INTERNAL_TOOL_PATTERN = re.compile(r"^_(.+)$")


def _get_tool_suffix(internal_name: str) -> str:
    """Extract suffix from internal tool name (_query -> query)."""
    match = INTERNAL_TOOL_PATTERN.match(internal_name)
    return match.group(1) if match else internal_name


def _inject_api_context(description: str, hostname: str, api_type: str) -> str:
    """Inject API context into tool description using full hostname."""
    api_type_label = "GraphQL" if api_type == "graphql" else "REST"
    prefix = f"[{hostname} {api_type_label} API] "
    return prefix + description


class DynamicToolNamingMiddleware(Middleware):
    """Middleware that dynamically names tools based on session context."""

    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next,
    ) -> Sequence[FastMCPTool]:
        """Transform tool names and descriptions based on session headers."""
        tools: Sequence[FastMCPTool] = await call_next(context)

        try:
            headers = get_http_headers()
        except LookupError:
            # No HTTP context (e.g., stdio transport) - return unchanged
            return tools

        target_url = headers.get("x-target-url", "")
        api_type = headers.get("x-api-type", "api")

        # Short prefix for tool name, full hostname for description
        name_prefix = extract_api_name(headers)
        full_hostname = get_full_hostname(target_url)

        transformed = []
        for tool in tools:
            suffix = _get_tool_suffix(tool.name)
            new_name = f"{name_prefix}_{suffix}"
            new_desc = _inject_api_context(tool.description or "", full_hostname, api_type)

            modified_tool = tool.model_copy(update={"name": new_name, "description": new_desc})
            transformed.append(modified_tool)

        return transformed

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next,
    ) -> mt.CallToolResult:
        """Validate and transform tool name back to internal name."""
        try:
            headers = get_http_headers()
        except LookupError:
            # No HTTP context - pass through unchanged
            return await call_next(context)

        api_name = extract_api_name(headers)
        tool_name = context.message.name

        # Validate tool name matches session's API
        expected_prefix = f"{api_name}_"
        if not tool_name.startswith(expected_prefix):
            from mcp.types import TextContent

            return mt.CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text=f"Tool '{tool_name}' not valid for API '{api_name}'. "
                        f"Expected tool name starting with '{expected_prefix}'.",
                    )
                ],
                isError=True,
            )

        # Transform back to internal name (_suffix)
        suffix = tool_name[len(expected_prefix) :]
        internal_name = f"_{suffix}"

        # Create modified context with internal tool name
        modified_params = mt.CallToolRequestParams(
            name=internal_name,
            arguments=context.message.arguments,
        )
        modified_context = context.copy(message=modified_params)

        return await call_next(modified_context)
