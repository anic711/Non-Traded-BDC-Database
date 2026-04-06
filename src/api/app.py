"""FastAPI application factory with scheduler integration."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.config import settings
from src.api.routes import router
from src.api.routes_dashboard import router as dashboard_router
from src.scheduler import create_scheduler

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage scheduler lifecycle with the FastAPI app."""
    scheduler = create_scheduler()
    scheduler.start()
    logger.info(
        f"Scheduler started: updates at {settings.schedule_hour_1}:00 "
        f"and {settings.schedule_hour_2}:00 {settings.schedule_timezone}"
    )
    yield
    scheduler.shutdown()
    logger.info("Scheduler shut down")


app = FastAPI(
    title="Non-Traded BDC Metrics Database",
    description="Collects and serves metrics for non-traded BDCs from SEC EDGAR filings.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(dashboard_router)

# Serve static frontend files
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(str(FRONTEND_DIR / "index.html"))
