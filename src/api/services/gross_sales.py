"""Tab A: Total Gross Sales computation."""

from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, compute_yoy_growth, compute_trailing_3m_yoy,
    pct_of, build_bank,
)


async def get_gross_sales_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute gross sales grid data for all funds.

    Gross sales = monthly delta in cumulative_consideration from shares_issued,
    summed across all share classes and offering types per fund.
    """
    funds = await get_fund_list()
    tickers = [f["ticker"] for f in funds]
    fund_id_map = {f["id"]: f["ticker"] for f in funds}

    # Query cumulative consideration summed by (fund_id, as_of_date)
    async with async_session_factory() as session:
        result = await session.execute(text("""
            SELECT fund_id, as_of_date, SUM(cumulative_consideration) as total_cum
            FROM shares_issued
            WHERE cumulative_consideration IS NOT NULL
            GROUP BY fund_id, as_of_date
            ORDER BY fund_id, as_of_date
        """))
        rows = result.fetchall()

    # Group by fund_id, compute monthly deltas
    fund_cumulative = defaultdict(list)
    for fund_id, dt, cum in rows:
        ticker = fund_id_map.get(fund_id)
        if ticker:
            fund_cumulative[ticker].append((date.fromisoformat(str(dt)), float(cum)))

    # Compute monthly deltas per fund
    monthly_sales = {}  # {ticker: {date: value}}
    for ticker, data_points in fund_cumulative.items():
        data_points.sort()
        sales = {}
        for i in range(1, len(data_points)):
            d, cum = data_points[i]
            _, cum_prev = data_points[i - 1]
            delta = cum - cum_prev
            if delta >= 0:  # negative deltas are data artifacts
                sales[d] = delta
        monthly_sales[ticker] = sales

    # Get total NAV for % of NAV
    nav_lookup = await get_total_nav_lookup()
    nav_by_ticker = {
        fund_id_map[fid]: navs for fid, navs in nav_lookup.items()
        if fid in fund_id_map
    }

    # Collect all dates that have data, filter to requested range
    all_data_dates = set()
    for sales in monthly_sales.values():
        all_data_dates.update(sales.keys())

    if period == "quarterly":
        for ticker in tickers:
            if ticker in monthly_sales:
                monthly_sales[ticker] = aggregate_quarterly(monthly_sales[ticker])
        # Re-collect dates after aggregation
        all_data_dates = set()
        for sales in monthly_sales.values():
            all_data_dates.update(sales.keys())

    dates = sorted(d for d in all_data_dates if start <= d <= end)

    # Compute sub-banks
    yoy_data = {t: compute_yoy_growth(monthly_sales.get(t, {})) for t in tickers}

    # For 3M trailing, we need the original monthly data even in quarterly mode
    if period == "quarterly":
        # Recompute from monthly for trailing
        monthly_raw = {}
        for ticker, data_points in fund_cumulative.items():
            data_points.sort()
            sales = {}
            for i in range(1, len(data_points)):
                d, cum = data_points[i]
                _, cum_prev = data_points[i - 1]
                delta = cum - cum_prev
                if delta >= 0:
                    sales[d] = delta
            monthly_raw[ticker] = sales
        trailing_data = {t: compute_trailing_3m_yoy(monthly_raw.get(t, {})) for t in tickers}
        # Map to quarter-end dates
        for t in tickers:
            quarterly_trailing = {}
            for d, v in trailing_data[t].items():
                if d in dates:
                    quarterly_trailing[d] = v
            trailing_data[t] = quarterly_trailing
    else:
        trailing_data = {t: compute_trailing_3m_yoy(monthly_sales.get(t, {})) for t in tickers}

    pct_nav_data = {}
    for t in tickers:
        pct_nav_data[t] = pct_of(monthly_sales.get(t, {}), nav_by_ticker.get(t, {}))

    banks = [
        build_bank("Total Gross Sales", "currency", monthly_sales, tickers, dates),
        build_bank("Y/Y Growth", "percent", yoy_data, tickers, dates),
        build_bank("Y/Y Growth - 3M Trailing", "percent", trailing_data, tickers, dates),
        build_bank("% of NAV", "percent", pct_nav_data, tickers, dates),
    ]

    return {"funds": tickers, "banks": banks}
