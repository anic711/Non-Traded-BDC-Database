"""FastAPI application factory with scheduler integration."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import settings
from src.api.routes import router
from src.scheduler import create_scheduler

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


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

app.include_router(router)
