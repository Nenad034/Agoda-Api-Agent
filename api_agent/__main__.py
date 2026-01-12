"""API Agent MCP Server - Universal GraphQL/REST to MCP gateway."""

import logging

import uvicorn
from fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

from .config import settings
from .middleware import DynamicToolNamingMiddleware
from .tools import register_all_tools

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_app():
    """Create MCP server application."""
    mcp = FastMCP(settings.MCP_NAME)
    register_all_tools(mcp)
    mcp.add_middleware(DynamicToolNamingMiddleware())

    cors_origins = [o.strip() for o in settings.CORS_ALLOWED_ORIGINS.split(",")]
    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
            expose_headers=[
                "Content-Type",
                "Authorization",
                "MCP-Session-Id",
                "mcp-protocol-version",
            ],
            allow_credentials=True,
            max_age=600,
        ),
    ]

    app = mcp.http_app(middleware=middleware, transport=settings.TRANSPORT)

    async def health(request):
        return JSONResponse({"status": "ok"})

    app.router.routes.append(Route("/health", health, methods=["GET"]))
    return app


def main():
    """Run server."""
    logger.info(f"Starting API Agent on {settings.HOST}:{settings.PORT}")
    logger.info("Endpoint config via headers: X-Target-URL, X-API-Type, X-Target-Headers")
    uvicorn.run(create_app(), host=settings.HOST, port=settings.PORT, log_level="info")


if __name__ == "__main__":
    main()
