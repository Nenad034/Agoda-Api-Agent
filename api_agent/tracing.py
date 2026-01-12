"""OpenTelemetry tracing setup."""

import logging
import os
from contextlib import contextmanager
from typing import Any, Generator

from .config import settings

logger = logging.getLogger(__name__)

_tracer_ready = False
_using_metadata_fn = None


def init_tracing() -> None:
    """Initialize tracing if OTLP endpoint available."""
    global _tracer_ready, _using_metadata_fn

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not otlp_endpoint:
        return

    try:
        from openinference.instrumentation import using_metadata
        from openinference.instrumentation.openai_agents import OpenAIAgentsInstrumentor
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        otlp_endpoint = otlp_endpoint.rstrip("/")
        resource = Resource.create({"service.name": settings.SERVICE_NAME})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces"))
        )
        trace.set_tracer_provider(provider)
        OpenAIAgentsInstrumentor().instrument(tracer_provider=provider)

        _using_metadata_fn = using_metadata
        _tracer_ready = True
        logger.info(f"Tracing enabled: {otlp_endpoint}")
    except Exception as e:
        logger.warning(f"Failed to setup tracing: {e}")


@contextmanager
def trace_metadata(metadata: dict[str, Any]) -> Generator[None, None, None]:
    """Context manager for span metadata. No-op if tracing disabled."""
    if _tracer_ready and _using_metadata_fn:
        with _using_metadata_fn(metadata):
            yield
    else:
        yield


def is_enabled() -> bool:
    """Check if tracing is enabled."""
    return _tracer_ready
