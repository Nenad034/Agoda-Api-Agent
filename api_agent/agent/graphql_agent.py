"""GraphQL agent using declarative queries (GraphQL + DuckDB SQL)."""

import json
import logging
import re
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
from ..graphql import execute_query as graphql_fetch
from ..tracing import trace_metadata
from .model import get_run_config, model
from .progress import get_turn_context, reset_progress
from .prompts import (
    CONTEXT_SECTION,
    EFFECTIVE_PATTERNS,
    GRAPHQL_SCHEMA_NOTATION,
    OPTIONAL_PARAMS_SPEC,
    PERSISTENCE_SPEC,
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
_graphql_queries: ContextVar[list[str]] = ContextVar("graphql_queries")
_query_results: ContextVar[dict[str, Any]] = ContextVar("query_results")
_last_result: ContextVar[list] = ContextVar("last_result")  # Mutable container: [result_value]
_raw_schema: ContextVar[str] = ContextVar("raw_schema")  # Raw introspection JSON for search


def _format_type(t: dict | None) -> str:
    """Convert introspection type to compact notation: [User!]!"""
    if not t:
        return "?"
    kind = t.get("kind")
    name = t.get("name")
    inner = t.get("ofType")

    if kind == "NON_NULL":
        return f"{_format_type(inner)}!"
    if kind == "LIST":
        return f"[{_format_type(inner)}]"
    return name or "?"


_INTROSPECTION_QUERY = """{
  __schema {
    queryType {
      fields { name description args { name type { ...TypeRef } defaultValue } type { ...TypeRef } }
    }
    types {
      name kind description
      fields { name description args { name type { ...TypeRef } defaultValue } type { ...TypeRef } }
      enumValues { name description }
      inputFields { name type { ...TypeRef } defaultValue }
      interfaces { name }
      possibleTypes { name }
    }
  }
}
fragment TypeRef on __Type {
  name kind ofType { name kind ofType { name kind ofType { name } } }
}"""


def _is_required(type_def: dict | None) -> bool:
    """Check if GraphQL type is required (NON_NULL wrapper)."""
    return type_def.get("kind") == "NON_NULL" if type_def else False


def _format_arg(a: dict) -> str:
    """Format argument with optional default value."""
    type_str = _format_type(a["type"])
    default = a.get("defaultValue")
    if default is not None:
        return f"{a['name']}: {type_str} = {default}"
    return f"{a['name']}: {type_str}"


def _filter_required_args(args: list[dict]) -> list[dict]:
    """Filter to only required arguments (NON_NULL type)."""
    return [a for a in args if _is_required(a.get("type"))]


def _format_field(fld: dict) -> str:
    """Format a field with optional args."""
    args = fld.get("args", [])
    if args:
        arg_str = "(" + ", ".join(_format_arg(a) for a in args) + ")"
    else:
        arg_str = ""
    desc = f" # {fld['description']}" if fld.get("description") else ""
    return f"  {fld['name']}{arg_str}: {_format_type(fld['type'])}{desc}"


def _build_schema_context(schema: dict) -> str:
    """Build compact SDL context from introspection schema."""
    queries = schema.get("queryType", {}).get("fields", [])
    all_types = [t for t in schema.get("types", []) if not t["name"].startswith("__")]

    objects = [
        t
        for t in all_types
        if t["kind"] == "OBJECT" and t["name"] not in ("Query", "Mutation", "Subscription")
    ]
    enums = [t for t in all_types if t["kind"] == "ENUM"]
    inputs = [t for t in all_types if t["kind"] == "INPUT_OBJECT"]
    interfaces = [t for t in all_types if t["kind"] == "INTERFACE"]
    unions = [t for t in all_types if t["kind"] == "UNION"]

    lines = ["<queries>"]
    for f in queries:
        desc = f" # {f['description']}" if f.get("description") else ""
        # Only show required args
        required_args = _filter_required_args(f.get("args", []))
        args = ", ".join(_format_arg(a) for a in required_args)
        lines.append(f"{f['name']}({args}) -> {_format_type(f['type'])}{desc}")

    if interfaces:
        lines.append("\n<interfaces>")
        for t in interfaces:
            impl = [p["name"] for p in t.get("possibleTypes", []) or []]
            impl_str = f" # implemented by: {', '.join(impl)}" if impl else ""
            fields = [_format_field(fld) for fld in t.get("fields", []) or []]
            lines.append(f"{t['name']} {{{impl_str}\n" + "\n".join(fields) + "\n}")

    if unions:
        lines.append("\n<unions>")
        for t in unions:
            types = [p["name"] for p in t.get("possibleTypes", []) or []]
            lines.append(f"{t['name']}: {' | '.join(types)}")

    lines.append("\n<types>")
    for t in objects:
        impl = [i["name"] for i in t.get("interfaces", []) or []]
        impl_str = f" implements {', '.join(impl)}" if impl else ""
        fields = [_format_field(fld) for fld in t.get("fields", []) or []]
        lines.append(f"{t['name']}{impl_str} {{\n" + "\n".join(fields) + "\n}")

    lines.append("\n<enums>")
    for e in enums:
        vals = " | ".join(v["name"] for v in e.get("enumValues", []))
        lines.append(f"{e['name']}: {vals}")

    lines.append("\n<inputs>")
    for inp in inputs:
        # Only show required input fields
        required_fields = [
            f for f in (inp.get("inputFields", []) or []) if _is_required(f.get("type"))
        ]
        fields = ", ".join(f"{f['name']}: {_format_type(f['type'])}" for f in required_fields)
        lines.append(f"{inp['name']} {{ {fields} }}")

    return "\n".join(lines)


def _strip_descriptions(context: str) -> str:
    """Strip # comments from SDL context."""
    return re.sub(r" #[^\n]*", "", context)


async def _fetch_schema_context(endpoint: str, headers: dict[str, str] | None) -> str:
    """Fetch schema in compact SDL format."""
    result = await graphql_fetch(_INTROSPECTION_QUERY, None, endpoint, headers)
    if not result.get("success") or not result.get("data"):
        return ""

    schema = result["data"]["__schema"]

    # Store raw introspection JSON for grep-like search (preserves all info)
    _raw_schema.set(json.dumps(schema, indent=2))

    # Build DSL for LLM context
    context = _build_schema_context(schema)

    if len(context) > settings.MAX_SCHEMA_CHARS:
        context = _strip_descriptions(context)
        if len(context) > settings.MAX_SCHEMA_CHARS:
            context = (
                context[: settings.MAX_SCHEMA_CHARS]
                + "\n[SCHEMA TRUNCATED - use search_schema() to explore]"
            )

    return context


def _build_system_prompt() -> str:
    """Build system prompt for GraphQL agent."""
    current_date = datetime.now().strftime("%Y-%m-%d")

    return f"""You are a GraphQL API agent that answers questions by querying APIs and returning data.

{CONTEXT_SECTION.format(current_date=current_date, max_turns=settings.MAX_AGENT_TURNS)}

{SQL_RULES}

## GraphQL-Specific
- Use inline values, never $variables

<tools>
graphql_query(query, name?)
  Execute GraphQL query. Result stored as DuckDB table.

{SQL_TOOL_DESC}

{SEARCH_TOOL_DESC}
</tools>

<workflow>
1. Read <queries> and <types> provided below
2. Execute graphql_query with needed fields
3. If user needs filtering/aggregation → sql_query, else return data
</workflow>

{GRAPHQL_SCHEMA_NOTATION}

{UNCERTAINTY_SPEC}

{OPTIONAL_PARAMS_SPEC}

{PERSISTENCE_SPEC.format(max_turns=settings.MAX_AGENT_TURNS)}

{EFFECTIVE_PATTERNS}

<examples>
Simple: graphql_query('{{ users(limit: 10) {{ id name }} }}')
Aggregation: graphql_query('{{ posts {{ authorId views }} }}'); sql_query('SELECT authorId, SUM(views) as total FROM data GROUP BY authorId')
Join: graphql_query('{{ users {{ id name }} }}', name='u'); graphql_query('{{ posts {{ authorId title }} }}', name='p'); sql_query('SELECT u.name, p.title FROM u JOIN p ON u.id = p.authorId')
</examples>
"""


def _create_graphql_query_tool(ctx: RequestContext):
    """Create graphql_query tool with bound context."""

    @function_tool
    async def graphql_query(query: str, name: str = "data") -> str:
        """Execute GraphQL query and store result for sql_query.

        Args:
            query: GraphQL query string
            name: Table name for sql_query (default: "data")

        Returns:
            JSON string with query results
        """
        result = await graphql_fetch(query, None, ctx.target_url, ctx.target_headers)

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

        try:
            _graphql_queries.get().append(query)
        except LookupError:
            pass

        if settings.DEBUG:
            logger.info(f"[Agent] GraphQL result: {json.dumps(result)[:500]}")

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

        return json.dumps(result, indent=2)

    return graphql_query


# Create search implementation bound to GraphQL schema context var
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


@function_tool
def sql_query(sql: str) -> str:
    """Run DuckDB SQL on stored GraphQL results.

    Args:
        sql: DuckDB SQL query

    Returns:
        JSON string with query results
    """
    try:
        data = _query_results.get()
    except LookupError:
        return json.dumps({"success": False, "error": "No data. Call graphql_query first."})

    if not data:
        return json.dumps({"success": False, "error": "No data. Call graphql_query first."})

    result = execute_sql(data, sql)

    if settings.DEBUG:
        logger.info(f"[Agent] SQL result: {json.dumps(result)[:500]}")

    # Store full result for final response + apply char truncation for LLM
    if result.get("success"):
        rows = result.get("result", [])
        # Mutate in-place so changes propagate from task group child
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


async def process_query(question: str, ctx: RequestContext) -> dict[str, Any]:
    """Process natural language query against GraphQL API.

    Args:
        question: Natural language question
        ctx: Request context with target_url and target_headers
    """
    try:
        if settings.DEBUG:
            logger.info(f"[Agent] Query: {question}")

        # Reset per-request storage
        # Use mutable containers so tool functions can modify in-place
        # (ContextVar.set() in child tasks doesn't propagate to parent)
        _graphql_queries.set([])
        _query_results.set({})
        _last_result.set([None])  # Mutable list: [result_value]
        reset_progress()  # Reset turn counter

        # Fetch schema with dynamic endpoint
        schema_ctx = await _fetch_schema_context(ctx.target_url, ctx.target_headers)

        # Create tools with bound context
        gql_tool = _create_graphql_query_tool(ctx)

        # Create fresh agent with dynamic tools
        agent = Agent(
            name="graphql-agent",
            model=model,
            instructions=_build_system_prompt(),
            tools=[gql_tool, sql_query, search_schema],
        )

        # Inject schema into query
        augmented_query = f"{schema_ctx}\n\nQuestion: {question}" if schema_ctx else question

        # Run agent with MaxTurnsExceeded handling for partial results
        queries = []
        last_data = None
        turn_info = ""
        try:
            with trace_metadata({"mcp_name": settings.MCP_SLUG, "agent_type": "graphql"}):
                result = await Runner.run(
                    agent,
                    augmented_query,
                    max_turns=settings.MAX_AGENT_TURNS,
                    run_config=get_run_config(),
                )

            queries = _graphql_queries.get()
            last_data = _last_result.get()[0]
            turn_info = get_turn_context(settings.MAX_AGENT_TURNS)

            if not result.final_output:
                if last_data:
                    return {
                        "ok": True,
                        "data": f"[Partial - {turn_info}] Data retrieved but agent didn't complete.",
                        "result": last_data,
                        "queries": queries,
                        "error": None,
                    }
                return {
                    "ok": False,
                    "data": None,
                    "result": None,
                    "queries": queries,
                    "error": f"No output ({turn_info})",
                }

            agent_output = str(result.final_output)

        except MaxTurnsExceeded:
            # Return partial results when turn limit exceeded
            queries = _graphql_queries.get()
            last_data = _last_result.get()[0]
            turn_info = get_turn_context(settings.MAX_AGENT_TURNS)

            if last_data:
                return {
                    "ok": True,
                    "data": f"[Partial - {turn_info}] Max turns exceeded but data retrieved.",
                    "result": last_data,
                    "queries": queries,
                    "error": None,
                }
            return {
                "ok": False,
                "data": None,
                "result": None,
                "queries": queries,
                "error": f"Max turns exceeded ({turn_info}), no data retrieved",
            }

        if settings.DEBUG:
            logger.info(f"[Agent] Output: {agent_output[:500]}")
            logger.info(f"[Agent] GraphQL queries: {len(queries)}")

        return {
            "ok": True,
            "data": agent_output,
            "result": last_data,
            "queries": queries,
            "error": None,
        }

    except Exception as e:
        logger.exception("Agent error")
        return {
            "ok": False,
            "data": None,
            "queries": [],
            "error": str(e),
        }
