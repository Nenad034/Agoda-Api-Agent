"""Tests for dynamic tool naming middleware."""

from api_agent.middleware import (
    _get_tool_suffix,
    _inject_api_context,
)


class TestGetToolSuffix:
    """Test internal tool name suffix extraction."""

    def test_underscore_prefix_query(self):
        assert _get_tool_suffix("_query") == "query"

    def test_underscore_prefix_execute(self):
        assert _get_tool_suffix("_execute") == "execute"

    def test_no_underscore_prefix(self):
        assert _get_tool_suffix("query") == "query"

    def test_double_underscore(self):
        assert _get_tool_suffix("__private") == "_private"


class TestInjectApiContext:
    """Test description injection with full hostname."""

    def test_rest_api_context(self):
        desc = "Ask questions in natural language."
        result = _inject_api_context(desc, "flights-api.example.com", "rest")
        assert result == "[flights-api.example.com REST API] Ask questions in natural language."

    def test_graphql_api_context(self):
        desc = "Query the API."
        result = _inject_api_context(desc, "catalog-graphql.example.com", "graphql")
        assert result == "[catalog-graphql.example.com GraphQL API] Query the API."

    def test_empty_description(self):
        result = _inject_api_context("", "api.example.com", "rest")
        assert result == "[api.example.com REST API] "


class TestToolTransformation:
    """Test tool name and description transformation."""

    def test_tool_name_with_prefix(self):
        """Verify tool names use prefix + suffix format."""
        prefix = "flights_api_example"
        internal_name = "_query"
        suffix = _get_tool_suffix(internal_name)
        expected = f"{prefix}_{suffix}"
        assert expected == "flights_api_example_query"
        assert len(expected) <= 32 + 6 + 1  # prefix(32) + suffix + underscore

    def test_description_includes_full_hostname(self):
        """Verify descriptions include full hostname."""
        hostname = "flights-api-qa.internal.example.com"
        result = _inject_api_context("Test.", hostname, "rest")
        assert hostname in result
        assert "REST API" in result
