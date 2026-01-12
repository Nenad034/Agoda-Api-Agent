"""Tests for SDL schema context generation."""

import pytest

from api_agent.agent.graphql_agent import (
    _build_schema_context,
    _format_arg,
    _format_field,
    _format_type,
)


class TestFormatType:
    """Test SDL type formatting."""

    def test_scalar(self):
        assert _format_type({"name": "String", "kind": "SCALAR"}) == "String"

    def test_non_null(self):
        t = {"kind": "NON_NULL", "ofType": {"name": "String", "kind": "SCALAR"}}
        assert _format_type(t) == "String!"

    def test_list(self):
        t = {"kind": "LIST", "ofType": {"name": "User", "kind": "OBJECT"}}
        assert _format_type(t) == "[User]"

    def test_non_null_list(self):
        t = {
            "kind": "NON_NULL",
            "ofType": {"kind": "LIST", "ofType": {"name": "User", "kind": "OBJECT"}},
        }
        assert _format_type(t) == "[User]!"

    def test_list_non_null(self):
        t = {
            "kind": "LIST",
            "ofType": {"kind": "NON_NULL", "ofType": {"name": "User", "kind": "OBJECT"}},
        }
        assert _format_type(t) == "[User!]"

    def test_deeply_nested(self):
        t = {
            "kind": "NON_NULL",
            "ofType": {
                "kind": "LIST",
                "ofType": {
                    "kind": "NON_NULL",
                    "ofType": {
                        "kind": "LIST",
                        "ofType": {
                            "kind": "NON_NULL",
                            "ofType": {"name": "User", "kind": "OBJECT"},
                        },
                    },
                },
            },
        }
        assert _format_type(t) == "[[User!]!]!"

    def test_none(self):
        assert _format_type(None) == "?"

    def test_empty(self):
        assert _format_type({}) == "?"


class TestFormatArg:
    """Test argument formatting with default values."""

    def test_arg_no_default(self):
        arg = {"name": "limit", "type": {"name": "Int", "kind": "SCALAR"}}
        assert _format_arg(arg) == "limit: Int"

    def test_arg_with_default(self):
        arg = {"name": "limit", "type": {"name": "Int", "kind": "SCALAR"}, "defaultValue": "10"}
        assert _format_arg(arg) == "limit: Int = 10"

    def test_arg_with_string_default(self):
        arg = {
            "name": "order",
            "type": {"name": "String", "kind": "SCALAR"},
            "defaultValue": '"ASC"',
        }
        assert _format_arg(arg) == 'order: String = "ASC"'

    def test_arg_with_non_null_type(self):
        arg = {
            "name": "id",
            "type": {"kind": "NON_NULL", "ofType": {"name": "ID", "kind": "SCALAR"}},
        }
        assert _format_arg(arg) == "id: ID!"

    def test_arg_with_list_type_and_default(self):
        arg = {
            "name": "statuses",
            "type": {"kind": "LIST", "ofType": {"name": "Status", "kind": "ENUM"}},
            "defaultValue": "[ACTIVE]",
        }
        assert _format_arg(arg) == "statuses: [Status] = [ACTIVE]"


class TestFormatField:
    """Test field formatting with args."""

    def test_field_no_args(self):
        fld = {
            "name": "id",
            "args": [],
            "type": {"kind": "NON_NULL", "ofType": {"name": "ID", "kind": "SCALAR"}},
        }
        assert _format_field(fld) == "  id: ID!"

    def test_field_with_args(self):
        fld = {
            "name": "components",
            "args": [
                {"name": "type", "type": {"name": "Type", "kind": "ENUM"}},
                {"name": "first", "type": {"name": "Int", "kind": "SCALAR"}},
            ],
            "type": {"kind": "LIST", "ofType": {"name": "Component", "kind": "INTERFACE"}},
        }
        assert _format_field(fld) == "  components(type: Type, first: Int): [Component]"

    def test_field_with_description(self):
        fld = {
            "name": "team",
            "args": [],
            "type": {"name": "Team", "kind": "OBJECT"},
            "description": "Owner team",
        }
        assert _format_field(fld) == "  team: Team # Owner team"


class TestBuildSchemaContext:
    """Test SDL context generation."""

    @pytest.fixture
    def sample_schema(self):
        """Realistic GraphQL schema fixture."""
        return {
            "queryType": {
                "fields": [
                    {
                        "name": "components",
                        "description": "List components",
                        "args": [
                            {
                                "name": "names",
                                "type": {
                                    "kind": "LIST",
                                    "ofType": {
                                        "kind": "NON_NULL",
                                        "ofType": {"name": "String", "kind": "SCALAR"},
                                    },
                                },
                            },
                            {"name": "type", "type": {"name": "Type", "kind": "ENUM"}},
                        ],
                        "type": {
                            "kind": "NON_NULL",
                            "ofType": {
                                "kind": "LIST",
                                "ofType": {
                                    "kind": "NON_NULL",
                                    "ofType": {"name": "Component", "kind": "INTERFACE"},
                                },
                            },
                        },
                    },
                    {
                        "name": "teams",
                        "description": None,
                        "args": [
                            {
                                "name": "ids",
                                "type": {
                                    "kind": "LIST",
                                    "ofType": {"name": "ID", "kind": "SCALAR"},
                                },
                            }
                        ],
                        "type": {"kind": "LIST", "ofType": {"name": "Team", "kind": "OBJECT"}},
                    },
                ]
            },
            "types": [
                # Interface: Component
                {
                    "name": "Component",
                    "kind": "INTERFACE",
                    "description": "Base component interface",
                    "fields": [
                        {
                            "name": "id",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "ID", "kind": "SCALAR"},
                            },
                        },
                        {
                            "name": "name",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "String", "kind": "SCALAR"},
                            },
                        },
                        {"name": "team", "args": [], "type": {"name": "Team", "kind": "OBJECT"}},
                        {
                            "name": "repositories",
                            "description": "Code repositories",
                            "args": [
                                {"name": "search", "type": {"name": "String", "kind": "SCALAR"}},
                                {"name": "first", "type": {"name": "Int", "kind": "SCALAR"}},
                            ],
                            "type": {"name": "ProjectConnection", "kind": "OBJECT"},
                        },
                    ],
                    "possibleTypes": [{"name": "Service"}, {"name": "Job"}, {"name": "Library"}],
                },
                # Union: ApprovalChange
                {
                    "name": "ApprovalChange",
                    "kind": "UNION",
                    "possibleTypes": [{"name": "RequestToDelete"}, {"name": "RequestToUpdate"}],
                },
                # Object: Service (implements Component)
                {
                    "name": "Service",
                    "kind": "OBJECT",
                    "interfaces": [{"name": "Component"}],
                    "fields": [
                        {
                            "name": "id",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "ID", "kind": "SCALAR"},
                            },
                        },
                        {
                            "name": "name",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "String", "kind": "SCALAR"},
                            },
                        },
                        {"name": "team", "args": [], "type": {"name": "Team", "kind": "OBJECT"}},
                        {
                            "name": "endpoint",
                            "args": [],
                            "type": {"name": "String", "kind": "SCALAR"},
                            "description": "API endpoint",
                        },
                    ],
                },
                # Object: Team
                {
                    "name": "Team",
                    "kind": "OBJECT",
                    "fields": [
                        {
                            "name": "id",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "ID", "kind": "SCALAR"},
                            },
                        },
                        {
                            "name": "name",
                            "args": [],
                            "type": {
                                "kind": "NON_NULL",
                                "ofType": {"name": "String", "kind": "SCALAR"},
                            },
                        },
                        {
                            "name": "components",
                            "args": [{"name": "type", "type": {"name": "Type", "kind": "ENUM"}}],
                            "type": {
                                "kind": "LIST",
                                "ofType": {"name": "Component", "kind": "INTERFACE"},
                            },
                        },
                    ],
                },
                # Enum: Type
                {
                    "name": "Type",
                    "kind": "ENUM",
                    "enumValues": [{"name": "Service"}, {"name": "Job"}, {"name": "Library"}],
                },
                # Enum: Status
                {
                    "name": "LifecycleStatus",
                    "kind": "ENUM",
                    "enumValues": [{"name": "ACTIVE"}, {"name": "DEPRECATED"}],
                },
                # Input: ComponentFilter
                {
                    "name": "ComponentFilter",
                    "kind": "INPUT_OBJECT",
                    "inputFields": [
                        {"name": "type", "type": {"name": "Type", "kind": "ENUM"}},
                        {"name": "teamId", "type": {"name": "ID", "kind": "SCALAR"}},
                    ],
                },
            ],
        }

    def test_queries_section(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<queries>" in ctx
        # Optional args stripped - names and type are not NON_NULL at top level
        assert "components() -> [Component!]! # List components" in ctx
        assert "teams() -> [Team]" in ctx

    def test_interfaces_section(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<interfaces>" in ctx
        assert "Component {" in ctx
        assert "# implemented by: Service, Job, Library" in ctx
        # Check nested field args
        assert (
            "repositories(search: String, first: Int): ProjectConnection # Code repositories" in ctx
        )

    def test_unions_section(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<unions>" in ctx
        assert "ApprovalChange: RequestToDelete | RequestToUpdate" in ctx

    def test_types_section_with_implements(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<types>" in ctx
        assert "Service implements Component {" in ctx
        assert "endpoint: String # API endpoint" in ctx

    def test_types_section_with_nested_args(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        # Team.components has args
        assert "components(type: Type): [Component]" in ctx

    def test_enums_section(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<enums>" in ctx
        assert "Type: Service | Job | Library" in ctx
        assert "LifecycleStatus: ACTIVE | DEPRECATED" in ctx

    def test_inputs_section(self, sample_schema):
        ctx = _build_schema_context(sample_schema)
        assert "<inputs>" in ctx
        # Optional fields stripped - type and teamId not NON_NULL
        assert "ComponentFilter {  }" in ctx

    def test_excludes_internal_types(self, sample_schema):
        sample_schema["types"].append({"name": "__Schema", "kind": "OBJECT", "fields": []})
        ctx = _build_schema_context(sample_schema)
        assert "__Schema" not in ctx

    def test_excludes_query_mutation_subscription(self, sample_schema):
        sample_schema["types"].append({"name": "Query", "kind": "OBJECT", "fields": []})
        sample_schema["types"].append({"name": "Mutation", "kind": "OBJECT", "fields": []})
        ctx = _build_schema_context(sample_schema)
        assert "\nQuery " not in ctx
        assert "\nMutation " not in ctx

    def test_empty_schema(self):
        ctx = _build_schema_context({})
        assert "<queries>" in ctx
        assert "<types>" in ctx
        assert "<enums>" in ctx
        assert "<inputs>" in ctx
