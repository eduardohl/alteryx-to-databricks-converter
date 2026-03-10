"""FastAPI application — serves API + React static build."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from a2d.__about__ import __version__
from server.routers import analyze, convert, health, history, tools, validate
from server.services.batch import get_store
from server.services import history as history_service
from server.settings import settings
from server.websocket import batch as ws_batch

logger = logging.getLogger("a2d.server")


async def _evict_expired_jobs() -> None:
    """Periodically remove expired batch jobs."""
    while True:
        await asyncio.sleep(300)  # every 5 minutes
        count = get_store().evict_expired()
        if count:
            logger.info("Evicted %d expired batch jobs", count)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging from settings
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Startup: ensure converters are loaded
    import a2d.converters  # noqa: F401

    # Initialize history database (optional)
    if settings.database_url:
        if history_service.init_db():
            logger.info("History database connected")
        else:
            logger.warning("History database configured but failed to initialize")
    else:
        logger.info("History database not configured — history feature disabled")

    logger.info("a2d API v%s starting up", __version__)
    cleanup_task = asyncio.create_task(_evict_expired_jobs())
    yield
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    logger.info("a2d API shutting down")


app = FastAPI(
    title="a2d API",
    description="Alteryx-to-Databricks Migration Accelerator API",
    version=__version__,
    lifespan=lifespan,
)

# CORS from settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=settings.cors_allow_methods,
    allow_headers=settings.cors_allow_headers,
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "status_code": 500},
    )


# API routers
app.include_router(health.router)
app.include_router(tools.router)
app.include_router(convert.router)
app.include_router(analyze.router)
app.include_router(history.router)
app.include_router(validate.router)

# WebSocket
app.include_router(ws_batch.router)

# Serve React build in production (after API routes so /api/* takes priority)
_frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="static")
