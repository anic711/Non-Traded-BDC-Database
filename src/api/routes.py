"""FastAPI route definitions."""

import asyncio
import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from src.database import async_session_factory
from src.models import (
    Fund, NavPerShare, Distribution, SharesIssued,
    Redemption, TotalNav, UpdateLog,
)
from src.api.schemas import (
    FundResponse, NavPerShareResponse, DistributionResponse,
    SharesIssuedResponse, RedemptionResponse, TotalNavResponse,
    UpdateTriggerResponse, UpdateStatusResponse, FundMetricsResponse,
)
from src.collectors.pipeline import run_update

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.get("/funds", response_model=list[FundResponse])
async def list_funds():
    async with async_session_factory() as session:
        result = await session.execute(select(Fund).order_by(Fund.ticker))
        funds = result.scalars().all()
    return funds


@router.get("/funds/{ticker}/metrics", response_model=FundMetricsResponse)
async def get_fund_metrics(
    ticker: str,
    date_from: date | None = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: date | None = Query(None, description="End date filter (YYYY-MM-DD)"),
):
    fund = await _get_fund(ticker)

    nav = await _query_metric(NavPerShare, fund.id, date_from, date_to)
    dist = await _query_metric(Distribution, fund.id, date_from, date_to)
    shares = await _query_metric(SharesIssued, fund.id, date_from, date_to)
    redemptions = await _query_metric(Redemption, fund.id, date_from, date_to)
    total_nav = await _query_metric(TotalNav, fund.id, date_from, date_to)

    return FundMetricsResponse(
        fund=FundResponse.model_validate(fund),
        nav_per_share=[NavPerShareResponse.model_validate(r) for r in nav],
        distributions=[DistributionResponse.model_validate(r) for r in dist],
        shares_issued=[SharesIssuedResponse.model_validate(r) for r in shares],
        redemptions=[RedemptionResponse.model_validate(r) for r in redemptions],
        total_nav=[TotalNavResponse.model_validate(r) for r in total_nav],
    )


@router.get("/funds/{ticker}/nav", response_model=list[NavPerShareResponse])
async def get_nav_per_share(
    ticker: str,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    fund = await _get_fund(ticker)
    records = await _query_metric(NavPerShare, fund.id, date_from, date_to)
    return [NavPerShareResponse.model_validate(r) for r in records]


@router.get("/funds/{ticker}/distributions", response_model=list[DistributionResponse])
async def get_distributions(
    ticker: str,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    fund = await _get_fund(ticker)
    records = await _query_metric(Distribution, fund.id, date_from, date_to)
    return [DistributionResponse.model_validate(r) for r in records]


@router.get("/funds/{ticker}/shares-issued", response_model=list[SharesIssuedResponse])
async def get_shares_issued(
    ticker: str,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    fund = await _get_fund(ticker)
    records = await _query_metric(SharesIssued, fund.id, date_from, date_to)
    return [SharesIssuedResponse.model_validate(r) for r in records]


@router.get("/funds/{ticker}/redemptions", response_model=list[RedemptionResponse])
async def get_redemptions(
    ticker: str,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    fund = await _get_fund(ticker)
    records = await _query_metric(Redemption, fund.id, date_from, date_to)
    return [RedemptionResponse.model_validate(r) for r in records]


@router.get("/funds/{ticker}/total-nav", response_model=list[TotalNavResponse])
async def get_total_nav(
    ticker: str,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    fund = await _get_fund(ticker)
    records = await _query_metric(TotalNav, fund.id, date_from, date_to)
    return [TotalNavResponse.model_validate(r) for r in records]


@router.post("/update/trigger", response_model=UpdateTriggerResponse)
async def trigger_update():
    """Manually trigger a data update. Runs in the background."""
    # Launch update in background task
    task = asyncio.create_task(run_update(trigger_type="manual"))

    return UpdateTriggerResponse(
        update_id=0,  # ID will be available via status endpoint
        status="started",
        message="Update triggered. Check /api/update/latest for status.",
    )


@router.get("/update/latest", response_model=UpdateStatusResponse | None)
async def get_latest_update_status():
    """Get the status of the most recent update."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(UpdateLog).order_by(UpdateLog.id.desc()).limit(1)
        )
        log = result.scalar_one_or_none()
    if not log:
        return None
    return UpdateStatusResponse.model_validate(log)


@router.get("/update/{update_id}", response_model=UpdateStatusResponse)
async def get_update_status(update_id: int):
    async with async_session_factory() as session:
        log = await session.get(UpdateLog, update_id)
    if not log:
        raise HTTPException(status_code=404, detail="Update not found")
    return UpdateStatusResponse.model_validate(log)


# --- Helpers ---

async def _get_fund(ticker: str) -> Fund:
    async with async_session_factory() as session:
        result = await session.execute(
            select(Fund).where(Fund.ticker == ticker.upper())
        )
        fund = result.scalar_one_or_none()
    if not fund:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")
    return fund


async def _query_metric(model, fund_id: int, date_from: date | None, date_to: date | None):
    async with async_session_factory() as session:
        query = select(model).where(model.fund_id == fund_id)
        if date_from:
            query = query.where(model.as_of_date >= date_from)
        if date_to:
            query = query.where(model.as_of_date <= date_to)
        query = query.order_by(model.as_of_date)
        result = await session.execute(query)
        return result.scalars().all()
