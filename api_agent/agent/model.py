"""Shared OpenAI model configuration for API agents."""

from agents import ModelSettings, RunConfig, set_default_openai_api, set_tracing_disabled
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from agents.run import CallModelData, ModelInputData
from openai import AsyncOpenAI
from openai.types.shared import Reasoning

from ..config import settings
from ..tracing import init_tracing
from ..tracing import is_enabled as tracing_enabled
from .progress import get_turn_context, increment_turn

# Set default API mode
set_default_openai_api("chat_completions")

# Initialize tracing
init_tracing()
if not tracing_enabled():
    set_tracing_disabled(True)

# Shared OpenAI client
client = AsyncOpenAI(
    api_key=settings.OPENAI_API_KEY,
    base_url=settings.OPENAI_BASE_URL,
)

# Shared model instance
model = OpenAIChatCompletionsModel(
    model=settings.MODEL_NAME,
    openai_client=client,
)


async def _inject_turn(call_data: CallModelData) -> ModelInputData:
    """Inject turn count into instructions before each LLM call."""
    increment_turn()
    turn_info = get_turn_context(settings.MAX_AGENT_TURNS)
    call_data.model_data.instructions += f"\n\n{turn_info}"
    return call_data.model_data


def get_run_config() -> RunConfig:
    """Get RunConfig with optional reasoning settings and turn injection."""
    model_settings = None
    if settings.REASONING_EFFORT:
        model_settings = ModelSettings(reasoning=Reasoning(effort=settings.REASONING_EFFORT))  # type: ignore[arg-type]

    return RunConfig(
        model_settings=model_settings,
        call_model_input_filter=_inject_turn,
    )
