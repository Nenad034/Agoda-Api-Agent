"""Shared prompt components for API agents."""

# Context section with date and limits
CONTEXT_SECTION = """<context>
Today's date: {current_date}
Max tool calls: {max_turns}
Use today's date to calculate relative dates (tomorrow, next week, etc.)
</context>"""

# SQL rules (shared)
SQL_RULES = """<sql-rules>
- API responses TRUNCATED; full data in DuckDB table
- sql_query for filtering, sorting, aggregation, joins
- Unique table names via 'name' param
- Structs: t.field.subfield (dot notation)
- Arrays: len(arr), arr[1] (1-indexed)
- UUIDs: CAST(id AS VARCHAR)
- UNNEST: FROM t, UNNEST(t.arr) AS u(val) → t.col for original, u.val for element
- EXCLUDE: SELECT * EXCLUDE (col) FROM t (not t.* EXCLUDE)
</sql-rules>"""

# Ambiguity handling
UNCERTAINTY_SPEC = """<uncertainty>
- Ambiguous query: state your interpretation, then answer
- Never fabricate figures—only report what API returned
</uncertainty>"""

# Optional parameters handling
OPTIONAL_PARAMS_SPEC = """<optional-params>
- Schema shows only required fields. Use search_schema to find optional fields.
- Don't invent values (IDs, usernames, etc.) - only use what user provides
</optional-params>"""

# Persistence on errors
PERSISTENCE_SPEC = """<persistence>
- If API call fails, analyze error and retry with corrected params
- Don't give up after first failure - adjust approach
- Use all {max_turns} turns if needed to complete task
</persistence>"""

# Effective patterns (reward good behaviors)
EFFECTIVE_PATTERNS = """<effective-patterns>
- Infer implicit params from user context
- Read schema for valid enum/type values
- Name tables descriptively
- Adapt SQL syntax on failure
- Use sensible defaults for pagination/limits
</effective-patterns>"""

# Tool descriptions
SQL_TOOL_DESC = """sql_query(sql)
  DuckDB SQL on stored tables. For filtering, sorting, aggregation, joins."""

SEARCH_TOOL_DESC = """search_schema(pattern, context=10, offset=0)
  Regex search on schema JSON. Returns matching lines with context.
  Use offset to paginate if results truncated."""

# Schema notation for REST
REST_SCHEMA_NOTATION = """<schema_notation>
METHOD /path(params) -> Type = endpoint signature
param?: Type = optional param | param: Type = required param
Type {{ field: type! }} = required field | {{ field: type }} = optional field
Type[] = array of Type
str(date-time) = ISO 8601 format: YYYY-MM-DDTHH:MM:SS
str(date) = ISO 8601 date: YYYY-MM-DD
</schema_notation>"""

# Schema notation for GraphQL
GRAPHQL_SCHEMA_NOTATION = """<schema_notation>
Type = object | Type! = non-null | [Type] = list
query(args) -> ReturnType = query signature
TypeName {{ field: Type }} = object fields
# comment = description
</schema_notation>"""
