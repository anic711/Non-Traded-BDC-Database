"""APScheduler setup for twice-daily automatic updates."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import settings
from src.collectors.pipeline import run_update

logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    scheduler = AsyncIOScheduler()

    # Schedule at configured hours in Eastern timezone
    scheduler.add_job(
        _scheduled_update,
        trigger=CronTrigger(
            hour=settings.schedule_hour_1,
            minute=0,
            timezone=settings.schedule_timezone,
        ),
        id="update_morning",
        name=f"BDC data update ({settings.schedule_hour_1}:00 ET)",
        replace_existing=True,
    )

    scheduler.add_job(
        _scheduled_update,
        trigger=CronTrigger(
            hour=settings.schedule_hour_2,
            minute=0,
            timezone=settings.schedule_timezone,
        ),
        id="update_evening",
        name=f"BDC data update ({settings.schedule_hour_2}:00 ET)",
        replace_existing=True,
    )

    return scheduler


async def _scheduled_update():
    """Wrapper for scheduled update runs."""
    logger.info("Starting scheduled update")
    try:
        log_id = await run_update(trigger_type="scheduled")
        logger.info(f"Scheduled update completed (log_id={log_id})")
    except Exception as e:
        logger.error(f"Scheduled update failed: {e}")
