"""Tab D: Redemption Requests computation."""

from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup, get_shares_outstanding_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, pct_of, build_bank, _closest_value,
)


async def get_redemption_requests_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute redemption requests grid data for all funds."""
    funds = await get_fund_list()
    tickers = [f["ticker"] for f in funds]
    fund_id_map = {f["id"]: f["ticker"] for f in funds}

    async with async_session_factory() as session:
        result = await session.execute(text("""
            SELECT fund_id, as_of_date, shares_tendered, shares_redeemed, value_redeemed
            FROM redemptions
            ORDER BY fund_id, as_of_date
        """))
        rows = result.fetchall()

        # Get average NAV per share for computing value of shares tendered
        nav_result = await session.execute(text("""
            SELECT fund_id, as_of_date, AVG(nav_per_share) as avg_nav
            FROM nav_per_share
            WHERE nav_per_share IS NOT NULL
            GROUP BY fund_id, as_of_date
        """))
        avg_nav_rows = nav_result.fetchall()

    avg_nav_by_fund = defaultdict(dict)
    for fund_id, dt, avg_nav in avg_nav_rows:
        avg_nav_by_fund[fund_id][date.fromisoformat(str(dt))] = float(avg_nav)

    shares_tendered = defaultdict(dict)
    value_tendered = defaultdict(dict)
    pct_fulfilled = defaultdict(dict)

    for fund_id, dt, tendered, redeemed, value_red in rows:
        ticker = fund_id_map.get(fund_id)
        if not ticker:
            continue
        d = date.fromisoformat(str(dt))

        if tendered is not None:
            t_val = float(tendered)
            shares_tendered[ticker][d] = t_val

            # Compute value of shares tendered = shares_tendered × avg NAV
            avg_nav = _closest_value(avg_nav_by_fund.get(fund_id, {}), d)
            if avg_nav:
                value_tendered[ticker][d] = t_val * avg_nav

            # % fulfilled = shares_redeemed / shares_tendered
            if redeemed is not None and t_val > 0:
                pct_fulfilled[ticker][d] = float(redeemed) / t_val

    nav_lookup = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_lookup.items() if fid in fund_id_map}
    so_lookup = await get_shares_outstanding_lookup()
    so_by_ticker = {fund_id_map[fid]: sos for fid, sos in so_lookup.items() if fid in fund_id_map}

    if period == "quarterly":
        for t in tickers:
            shares_tendered[t] = aggregate_quarterly(shares_tendered.get(t, {}))
            value_tendered[t] = aggregate_quarterly(value_tendered.get(t, {}))

    all_dates = set()
    for d in list(shares_tendered.values()) + list(value_tendered.values()):
        all_dates.update(d.keys())
    dates = sorted(d for d in all_dates if start <= d <= end)

    shares_pct_os = {t: pct_of(shares_tendered.get(t, {}), so_by_ticker.get(t, {})) for t in tickers}
    value_pct_nav = {t: pct_of(value_tendered.get(t, {}), nav_by_ticker.get(t, {})) for t in tickers}

    banks = [
        build_bank("Shares Tendered", "number", dict(shares_tendered), tickers, dates),
        build_bank("% of Shares O/S", "percent", shares_pct_os, tickers, dates),
        build_bank("Value of Shares Tendered", "currency", dict(value_tendered), tickers, dates),
        build_bank("% of NAV", "percent", value_pct_nav, tickers, dates),
        build_bank("% Fulfilled", "percent", dict(pct_fulfilled), tickers, dates),
    ]

    return {"funds": tickers, "banks": banks}
