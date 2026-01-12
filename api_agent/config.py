"""Configuration settings for API Agent MCP server."""

import re

from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings with API_AGENT_ env prefix. OPENAI_* also accepts unprefixed."""

    model_config = SettingsConfigDict(env_prefix="API_AGENT_", env_file=".env", extra="ignore")

    # MCP Server
    MCP_NAME: str = "API Agent"
    SERVICE_NAME: str = "api-agent"

    @computed_field
    @property
    def MCP_SLUG(self) -> str:
        """Slugified MCP_NAME for identifiers."""
        return re.sub(r"[^a-z0-9]+", "_", self.MCP_NAME.lower()).strip("_")

    # LLM (accepts both API_AGENT_OPENAI_* and OPENAI_*)
    OPENAI_API_KEY: str = Field(
        default="",
        validation_alias=AliasChoices("API_AGENT_OPENAI_API_KEY", "OPENAI_API_KEY"),
    )
    OPENAI_BASE_URL: str = Field(
        default="https://api.openai.com/v1",
        validation_alias=AliasChoices("API_AGENT_OPENAI_BASE_URL", "OPENAI_BASE_URL"),
    )
    MODEL_NAME: str = "gpt-5.2"
    REASONING_EFFORT: str = ""  # "low", "medium", "high" - empty = disabled

    # Agent limits
    MAX_AGENT_TURNS: int = 30
    MAX_RESPONSE_CHARS: int = 50000
    MAX_SCHEMA_CHARS: int = 32000
    MAX_PREVIEW_ROWS: int = 10  # Rows to show before suggesting pagination
    MAX_TOOL_RESPONSE_CHARS: int = 32000  # ~8K tokens, cap tool responses for LLM context

    # Polling limits
    MAX_POLLS: int = 20  # Max poll attempts
    DEFAULT_POLL_DELAY_MS: int = 3000  # Default delay if agent doesn't specify

    # Server
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 3000
    TRANSPORT: str = "streamable-http"
    CORS_ALLOWED_ORIGINS: str = "*"


settings = Settings()
