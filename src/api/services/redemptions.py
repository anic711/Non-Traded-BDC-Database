"""Tab B: Redemptions computation."""

from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup, get_shares_outstanding_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, compute_yoy_growth, pct_of, build_bank,
)


async def get_redemptions_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute redemptions grid data for all funds."""
    funds = await get_fund_list()
    tickers = [f["ticker"] for f in funds]
    fund_id_map = {f["id"]: f["ticker"] for f in funds}

    async with async_session_factory() as session:
        result = await session.execute(text("""
            SELECT fund_id, as_of_date, shares_redeemed, value_redeemed
            FROM redemptions
            ORDER BY fund_id, as_of_date
        """))
        rows = result.fetchall()

    shares_data = defaultdict(dict)  # {ticker: {date: value}}
    value_data = defaultdict(dict)
    for fund_id, dt, shares, value in rows:
        ticker = fund_id_map.get(fund_id)
        if ticker:
            d = date.fromisoformat(str(dt))
            if shares is not None:
                shares_data[ticker][d] = float(shares)
            if value is not None:
                value_data[ticker][d] = float(value)

    nav_lookup = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_lookup.items() if fid in fund_id_map}
    so_lookup = await get_shares_outstanding_lookup()
    so_by_ticker = {fund_id_map[fid]: sos for fid, sos in so_lookup.items() if fid in fund_id_map}

    if period == "quarterly":
        for t in tickers:
            shares_data[t] = aggregate_quarterly(shares_data.get(t, {}))
            value_data[t] = aggregate_quarterly(value_data.get(t, {}))

    all_dates = set()
    for d in list(shares_data.values()) + list(value_data.values()):
        all_dates.update(d.keys())
    dates = sorted(d for d in all_dates if start <= d <= end)

    # Sub-banks
    shares_yoy = {t: compute_yoy_growth(shares_data.get(t, {})) for t in tickers}
    value_yoy = {t: compute_yoy_growth(value_data.get(t, {})) for t in tickers}
    shares_pct_os = {t: pct_of(shares_data.get(t, {}), so_by_ticker.get(t, {})) for t in tickers}
    value_pct_nav = {t: pct_of(value_data.get(t, {}), nav_by_ticker.get(t, {})) for t in tickers}

    banks = [
        build_bank("Shares Redeemed", "number", dict(shares_data), tickers, dates),
        build_bank("Y/Y Growth (Shares)", "percent", shares_yoy, tickers, dates),
        build_bank("% of Shares O/S", "percent", shares_pct_os, tickers, dates),
        build_bank("Value of Shares Redeemed", "currency", dict(value_data), tickers, dates),
        build_bank("Y/Y Growth (Value)", "percent", value_yoy, tickers, dates),
        build_bank("% of NAV", "percent", value_pct_nav, tickers, dates),
    ]

    return {"funds": tickers, "banks": banks}
