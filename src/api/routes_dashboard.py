"""Dashboard API endpoints for the BDC metrics web application."""

import logging
from datetime import date, timedelta

from fastapi import APIRouter, Query

from src.api.services.gross_sales import get_gross_sales_data
from src.api.services.redemptions import get_redemptions_data
from src.api.services.performance import get_performance_data
from src.api.services.redemption_requests import get_redemption_requests_data

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard")


def _default_dates(
    start: str | None,
    end: str | None,
) -> tuple[date, date]:
    """Parse date params or return defaults (last 12 months)."""
    if end:
        end_date = date.fromisoformat(end + "-01") if len(end) == 7 else date.fromisoformat(end)
    else:
        end_date = date.today()

    if start:
        start_date = date.fromisoformat(start + "-01") if len(start) == 7 else date.fromisoformat(start)
    else:
        start_date = date(end_date.year - 1, end_date.month, 1)

    return start_date, end_date


@router.get("/gross-sales")
async def dashboard_gross_sales(
    start: str | None = Query(None, description="Start date (YYYY-MM or YYYY-MM-DD)"),
    end: str | None = Query(None, description="End date (YYYY-MM or YYYY-MM-DD)"),
    period: str = Query("monthly", description="monthly or quarterly"),
):
    start_date, end_date = _default_dates(start, end)
    return await get_gross_sales_data(start_date, end_date, period)


@router.get("/redemptions")
async def dashboard_redemptions(
    start: str | None = Query(None),
    end: str | None = Query(None),
    period: str = Query("monthly"),
):
    start_date, end_date = _default_dates(start, end)
    return await get_redemptions_data(start_date, end_date, period)


@router.get("/performance")
async def dashboard_performance(
    start: str | None = Query(None),
    end: str | None = Query(None),
    period: str = Query("monthly"),
):
    start_date, end_date = _default_dates(start, end)
    return await get_performance_data(start_date, end_date, period)


@router.get("/redemption-requests")
async def dashboard_redemption_requests(
    start: str | None = Query(None),
    end: str | None = Query(None),
    period: str = Query("monthly"),
):
    start_date, end_date = _default_dates(start, end)
    return await get_redemption_requests_data(start_date, end_date, period)
