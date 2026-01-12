"""REST agent using declarative queries (REST API + DuckDB SQL)."""

import asyncio
import json
import logging
from contextvars import ContextVar
from datetime import datetime
from typing import Any

from agents import Agent, MaxTurnsExceeded, Runner, function_tool

from ..config import settings
from ..context import RequestContext
from ..executor import (
    execute_sql,
    extract_tables_from_response,
    truncate_for_context,
)
from ..rest.client import execute_request
from ..rest.schema_loader import fetch_schema_context
from ..tracing import trace_metadata
from .model import get_run_config, model
from .progress import get_turn_context, reset_progress
from .prompts import (
    CONTEXT_SECTION,
    EFFECTIVE_PATTERNS,
    OPTIONAL_PARAMS_SPEC,
    PERSISTENCE_SPEC,
    REST_SCHEMA_NOTATION,
    SEARCH_TOOL_DESC,
    SQL_RULES,
    SQL_TOOL_DESC,
    UNCERTAINTY_SPEC,
)
from .schema_search import create_search_schema_impl

logger = logging.getLogger(__name__)


# Context-local storage (isolated per async request)
# NOTE: Use mutable containers for values that need to be modified by tool functions,
# because ContextVar.set() in child tasks (task groups) doesn't propagate to parent.
_rest_calls: ContextVar[list[dict[str, Any]]] = ContextVar("rest_calls")
_query_results: ContextVar[dict[str, Any]] = ContextVar("query_results")
_last_result: ContextVar[list] = ContextVar("last_result")  # Mutable container: [result_value]
_raw_schema: ContextVar[str] = ContextVar("raw_schema")  # Raw OpenAPI JSON for search


def _get_nested_value(data: dict | None, path: str) -> Any:
    """Extract value from nested dict/list using dot notation.

    Args:
        data: Dictionary to extract from
        path: Dot-separated path (e.g., "polling.completed", "trips.0.isCompleted")

    Returns:
        Value at path or None if not found
    """
    if not data or not path:
        return None
    keys = path.split(".")
    current: Any = data
    for key in keys:
        if not isinstance(current, (dict, list)):
            return None
        if isinstance(current, list) and key.isdigit():
            idx = int(key)
            if 0 <= idx < len(current):
                current = current[idx]
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(key)
        else:
            return None
        if current is None:
            return None
    return current


def _set_nested_value(data: dict, path: str, value: Any) -> None:
    """Set value in nested dict using dot notation, creating intermediate dicts.

    Args:
        data: Dictionary to modify
        path: Dot-separated path (e.g., "polling.count")
        value: Value to set
    """
    if not path:
        return
    keys = path.split(".")
    current = data
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _build_system_prompt(poll_paths: tuple[str, ...] = ()) -> str:
    """Build system prompt for REST agent.

    Args:
        poll_paths: Paths that require polling (empty = no polling support)
    """
    current_date = datetime.now().strftime("%Y-%m-%d")

    poll_tool_desc = ""
    poll_rules = ""
    if poll_paths:
        paths_str = ", ".join(poll_paths)
        poll_tool_desc = f"""
poll_until_done(method, path, done_field, done_value, body?, name?, delay_ms?)
  Poll async API until done_field equals done_value.
  - done_field: dot-path (e.g., "status", "data.0.complete", "trips.0.isCompleted")
  - done_value: target value as string ("true", "COMPLETED")
  - delay_ms: ms between polls (default: {settings.DEFAULT_POLL_DELAY_MS}ms)
  - Auto-increments polling.count if present in body
  Max {settings.MAX_POLLS} polls. Polling paths: {paths_str}
"""
        poll_rules = f"""
<polling-required>
IMPORTANT: These paths are ASYNC and REQUIRE polling: {paths_str}
- You MUST use poll_until_done (NOT rest_call) for these paths
- rest_call will fail or return incomplete data for polling paths
- Check schema for the completion field (e.g., isCompleted, status, done)
</polling-required>
"""

    # Conditionally add polling example
    poll_example = ""
    if poll_paths:
        poll_example = f"""Polling: poll_until_done("POST", "{poll_paths[0]}", done_field="isCompleted", done_value="true", body='{{...}}')
"""

    return f"""You are a REST API agent that answers questions by querying APIs and returning data.

{CONTEXT_SECTION.format(current_date=current_date, max_turns=settings.MAX_AGENT_TURNS)}

{SQL_RULES}

<tools>
rest_call(method, path, path_params?, query_params?, body?, name?)
  Execute REST call. Result stored as DuckDB table.
  - path_params: URL placeholders like {{id}} in /users/{{id}}
  - query_params: ?key=value params
  - body: JSON string for POST/PUT/PATCH
{poll_tool_desc}
{SQL_TOOL_DESC}

{SEARCH_TOOL_DESC}
</tools>

<workflow>
1. Read <endpoints> and <schemas> below
2. Check if endpoint is in polling paths - if yes, use poll_until_done; otherwise use rest_call
3. Use sql_query to filter/aggregate results
</workflow>

{REST_SCHEMA_NOTATION}
{poll_rules}
{UNCERTAINTY_SPEC}

{OPTIONAL_PARAMS_SPEC}

{PERSISTENCE_SPEC.format(max_turns=settings.MAX_AGENT_TURNS)}

{EFFECTIVE_PATTERNS}

<examples>
GET: rest_call("GET", "/users", query_params='{{"limit": 10}}')
Path param: rest_call("GET", "/users/{{{{id}}}}", path_params='{{"id": "123"}}')
{poll_example}Join: rest_call("GET", "/users", name="u"); rest_call("GET", "/posts", name="p"); sql_query('SELECT u.name, p.title FROM u JOIN p ON u.id = p.userId')
</examples>
"""


def _create_rest_call_tool(ctx: RequestContext, base_url: str):
    """Create rest_call tool with bound context."""

    @function_tool
    async def rest_call(
        method: str,
        path: str,
        path_params: str = "",
        query_params: str = "",
        body: str = "",
        name: str = "data",
    ) -> str:
        """Execute REST API call and store result for sql_query.

        Args:
            method: HTTP method (GET recommended, others may be blocked)
            path: API path (e.g., /users/{id})
            path_params: JSON string for path values (e.g., '{"id": "123"}')
            query_params: JSON string for query params (e.g., '{"limit": 10}')
            body: JSON string for request body (e.g., '{"name": "John"}')
            name: Table name for sql_query (default: "data")

        Returns:
            JSON string with API response
        """
        # Parse JSON params
        pp = json.loads(path_params) if path_params else None
        qp = json.loads(query_params) if query_params else None
        bd = json.loads(body) if body else None

        result = await execute_request(
            method,
            path,
            pp,
            qp,
            bd,
            base_url=base_url,
            headers=ctx.target_headers,
            allow_unsafe_paths=list(ctx.allow_unsafe_paths),
        )

        # Track call
        try:
            _rest_calls.get().append(
                {
                    "method": method,
                    "path": path,
                    "path_params": path_params,
                    "query_params": query_params,
                    "body": body,
                }
            )
        except LookupError:
            pass

        # Store result for sql_query
        schema_info = None
        stored_data = None
        if result.get("success"):
            try:
                results = _query_results.get()
                data = result.get("data", {})
                tables, schema_info = extract_tables_from_response(data, name)
                results.update(tables)
                _query_results.set(results)
                # Store full data for final response (the extracted list)
                # Mutate in-place so changes propagate from task group child
                stored_data = tables.get(name)
                if stored_data is not None:
                    _last_result.get()[0] = stored_data
            except LookupError:
                pass

        if settings.DEBUG:
            logger.info(f"[REST Agent] Result: {json.dumps(result)[:500]}")

        # Smart context optimization - cap by chars for LLM safety
        if result.get("success") and stored_data:
            # Wrapped dict (1-row) → return schema info
            if schema_info:
                return json.dumps(
                    {"success": True, "table": name, **schema_info},
                    indent=2,
                )

            # Apply char-based truncation (normalized format)
            if isinstance(stored_data, list):
                return json.dumps(
                    {"success": True, **truncate_for_context(stored_data, name)},
                    indent=2,
                )

        # Add hints on failure to guide agent recovery
        if not result.get("success"):
            status = result.get("status_code", 0)
            # HTTP 4xx/5xx errors - suggest schema search for valid values
            if status >= 400:
                result["hint"] = "Use search_schema to find valid enum values or field names"

        return json.dumps(result, indent=2)

    return rest_call


def _create_poll_tool(ctx: RequestContext, base_url: str):
    """Create poll_until_done tool with bound context."""

    @function_tool
    async def poll_until_done(
        method: str,
        path: str,
        done_field: str,
        done_value: str,
        body: str = "",
        path_params: str = "",
        query_params: str = "",
        name: str = "poll_result",
        delay_ms: int = 0,
    ) -> str:
        """Poll endpoint until done_field equals done_value. Auto-increments polling.count if present.

        Args:
            method: HTTP method (POST typically)
            path: API path
            done_field: Dot-path to check (e.g., "status", "polling.completed", "trips.0.isCompleted")
            done_value: Value indicating done (e.g., "true", "0", "COMPLETED", "100")
            body: JSON string request body
            path_params: JSON string for path values
            query_params: JSON string for query params
            name: Table name for sql_query (default: poll_result)
            delay_ms: Delay between polls in ms (default: 3000ms)

        Returns:
            JSON string with final response or error
        """
        pp = json.loads(path_params) if path_params else None
        qp = json.loads(query_params) if query_params else None
        try:
            body_dict = json.loads(body) if body else {}
        except json.JSONDecodeError as e:
            return json.dumps(
                {
                    "success": False,
                    "error": f"Invalid body JSON: {e.msg}",
                }
            )

        # Internal defaults from config
        max_polls = settings.MAX_POLLS
        wait_ms = delay_ms if delay_ms > 0 else settings.DEFAULT_POLL_DELAY_MS
        current = None  # Track last done_field value for error messages

        attempt = 0
        while attempt < max_polls:
            attempt += 1

            result = await execute_request(
                method,
                path,
                pp,
                qp,
                body=body_dict if body_dict else None,
                base_url=base_url,
                headers=ctx.target_headers,
                allow_unsafe_paths=list(ctx.allow_unsafe_paths),
            )

            # Track call
            try:
                _rest_calls.get().append(
                    {
                        "method": method,
                        "path": path,
                        "body": json.dumps(body_dict) if body_dict else "",
                        "poll_attempt": attempt,
                    }
                )
            except LookupError:
                pass

            if not result.get("success"):
                return json.dumps(
                    {
                        "success": False,
                        "error": result.get("error"),
                        "attempt": attempt,
                    }
                )

            data = result.get("data", {})

            # Validate done_field exists on first response
            current = _get_nested_value(data, done_field)
            if current is None and attempt == 1:
                keys = list(data.keys()) if isinstance(data, dict) else []
                return json.dumps(
                    {
                        "success": False,
                        "error": f"done_field '{done_field}' not found in response. Available keys: {keys}",
                    }
                )

            # Check if done_field value matches done_value (string comparison)
            is_done = str(current).lower() == done_value.lower()

            if is_done:
                # Store result for sql_query
                try:
                    results = _query_results.get()
                    tables, _ = extract_tables_from_response(data, name)
                    results.update(tables)
                    stored = tables.get(name)
                    if stored is not None:
                        _last_result.get()[0] = stored
                except LookupError:
                    pass

                return json.dumps(
                    {
                        "success": True,
                        **truncate_for_context(data if isinstance(data, list) else [data], name),
                        "attempts": attempt,
                    },
                    indent=2,
                )

            await asyncio.sleep(wait_ms / 1000)

            # Auto-increment polling.count if present in body
            if body_dict.get("polling", {}).get("count") is not None:
                body_dict["polling"]["count"] += 1

        return json.dumps(
            {
                "success": False,
                "error": f"max_polls ({max_polls}) exceeded. Last {done_field} value: {current} (expected: {done_value})",
                "attempts": attempt,
            }
        )

    return poll_until_done


@function_tool
def sql_query(sql: str) -> str:
    """Run DuckDB SQL on stored REST API results.

    Tables available = names from rest_call calls + auto-extracted top-level keys.

    Args:
        sql: DuckDB SQL query

    Returns:
        JSON string with query results
    """
    try:
        data = _query_results.get()
    except LookupError:
        return json.dumps({"success": False, "error": "No data. Call rest_call first."})

    if not data:
        return json.dumps({"success": False, "error": "No data. Call rest_call first."})

    result = execute_sql(data, sql)

    if settings.DEBUG:
        logger.info(f"[REST Agent] SQL result: {json.dumps(result)[:500]}")

    # Store full result for final response + apply char truncation for LLM
    if result.get("success"):
        rows = result.get("result", [])
        try:
            _last_result.get()[0] = rows
        except LookupError:
            pass

        if isinstance(rows, list):
            return json.dumps(
                {"success": True, **truncate_for_context(rows, "sql_result")},
                indent=2,
            )

    return json.dumps(result, indent=2)


# Create search implementation bound to REST schema context var
_search_schema_impl = create_search_schema_impl(_raw_schema)


@function_tool
def search_schema(
    pattern: str,
    context: int = 10,
    before: int = 0,
    after: int = 0,
    offset: int = 0,
) -> str:
    """Grep-like search on schema. Output: "line_num:match" or "line_num-context".

    Args:
        pattern: Regex pattern (case-insensitive)
        context: Lines around each match (default 10)
        before: Lines before match (overrides context)
        after: Lines after match (overrides context)
        offset: Number of matches to skip (for pagination)
    """
    return _search_schema_impl(
        pattern,
        before=before,
        after=after,
        context=context,
        offset=offset,
    )


async def process_rest_query(question: str, ctx: RequestContext) -> dict[str, Any]:
    """Process natural language query against REST API.

    Args:
        question: Natural language question
        ctx: Request context with target_url (OpenAPI spec) and target_headers
    """
    try:
        if settings.DEBUG:
            logger.info(f"[REST Agent] Query: {question}")

        # Reset per-request storage
        _rest_calls.set([])
        _query_results.set({})
        _last_result.set([None])  # Mutable list: [result_value]
        reset_progress()  # Reset turn counter

        # Fetch schema context (target_url = OpenAPI spec URL)
        schema_ctx, spec_base_url, raw_spec_json = await fetch_schema_context(
            ctx.target_url, ctx.target_headers
        )

        # Store raw OpenAPI spec for search_schema tool
        _raw_schema.set(raw_spec_json)

        # Use header override or spec-derived base URL
        base_url = ctx.base_url or spec_base_url
        if not base_url:
            return {
                "ok": False,
                "data": None,
                "api_calls": [],
                "error": "Could not determine base URL. Set X-Base-URL header or ensure spec has 'servers' field.",
            }

        # Create tools with bound context
        rest_tool = _create_rest_call_tool(ctx, base_url)

        # Only include poll tool if user specified poll_paths header
        include_polling = bool(ctx.poll_paths)
        tools = [rest_tool, sql_query, search_schema]
        if include_polling:
            poll_tool = _create_poll_tool(ctx, base_url)
            tools.insert(1, poll_tool)

        # Create fresh agent with dynamic tools
        agent = Agent(
            name="rest-agent",
            model=model,
            instructions=_build_system_prompt(poll_paths=ctx.poll_paths),
            tools=tools,
        )

        # Inject schema into query
        augmented_query = f"{schema_ctx}\n\nQuestion: {question}" if schema_ctx else question

        # Run agent with MaxTurnsExceeded handling for partial results
        api_calls = []
        last_data = None
        turn_info = ""
        try:
            with trace_metadata({"mcp_name": settings.MCP_SLUG, "agent_type": "rest"}):
                result = await Runner.run(
                    agent,
                    augmented_query,
                    max_turns=settings.MAX_AGENT_TURNS,
                    run_config=get_run_config(),
                )

            api_calls = _rest_calls.get()
            last_data = _last_result.get()[0]
            turn_info = get_turn_context(settings.MAX_AGENT_TURNS)

            if not result.final_output:
                if last_data:
                    return {
                        "ok": True,
                        "data": f"[Partial - {turn_info}] Data retrieved but agent didn't complete.",
                        "result": last_data,
                        "api_calls": api_calls,
                        "error": None,
                    }
                return {
                    "ok": False,
                    "data": None,
                    "result": None,
                    "api_calls": api_calls,
                    "error": f"No output ({turn_info})",
                }

            agent_output = str(result.final_output)

        except MaxTurnsExceeded:
            # Return partial results when turn limit exceeded
            api_calls = _rest_calls.get()
            last_data = _last_result.get()[0]
            turn_info = get_turn_context(settings.MAX_AGENT_TURNS)

            if last_data:
                return {
                    "ok": True,
                    "data": f"[Partial - {turn_info}] Max turns exceeded but data retrieved.",
                    "result": last_data,
                    "api_calls": api_calls,
                    "error": None,
                }
            return {
                "ok": False,
                "data": None,
                "result": None,
                "api_calls": api_calls,
                "error": f"Max turns exceeded ({turn_info}), no data retrieved",
            }

        if settings.DEBUG:
            logger.info(f"[REST Agent] Output: {agent_output[:500]}")
            logger.info(f"[REST Agent] API calls: {len(api_calls)}")

        return {
            "ok": True,
            "data": agent_output,
            "result": last_data,
            "api_calls": api_calls,
            "error": None,
        }

    except Exception as e:
        logger.exception("REST Agent error")
        return {
            "ok": False,
            "data": None,
            "api_calls": [],
            "error": str(e),
        }
