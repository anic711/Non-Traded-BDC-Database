"""Tab A: Total Gross Sales computation."""

from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, compute_yoy_growth, compute_trailing_3m_yoy,
    pct_of, build_bank, _closest_value, _prior_value,
)


def _is_rounded(value: float) -> bool:
    """Check if a consideration value is rounded to the nearest $100M."""
    return value % 100_000_000 == 0


def _compute_monthly_deltas(fund_cumulative, fund_ticker_to_id, nav_by_fund):
    """Compute monthly sales from cumulative data, with interpolation for gaps."""
    monthly_sales = {}
    for ticker, data_points in fund_cumulative.items():
        data_points.sort()
        sales = {}
        fid = fund_ticker_to_id[ticker]
        for i in range(1, len(data_points)):
            d, cum, shares = data_points[i]
            d_prev, cum_prev, shares_prev = data_points[i - 1]
            delta_consideration = cum - cum_prev
            if delta_consideration < 0:
                continue

            sale_amount = None
            if _is_rounded(cum) and _is_rounded(cum_prev):
                delta_shares = shares - shares_prev
                if delta_shares > 0:
                    prior_nav = _closest_value(nav_by_fund.get(fid, {}), d_prev)
                    if prior_nav:
                        sale_amount = delta_shares * prior_nav
            if sale_amount is None and delta_consideration >= 0:
                sale_amount = delta_consideration

            if sale_amount is None:
                continue

            # If data points span multiple months, split evenly
            months_gap = (d.year - d_prev.year) * 12 + (d.month - d_prev.month)
            if months_gap > 1:
                per_month = sale_amount / months_gap
                for m in range(1, months_gap + 1):
                    mo = d_prev.month + m
                    yr = d_prev.year + (mo - 1) // 12
                    mo = (mo - 1) % 12 + 1
                    sales[date(yr, mo, 1)] = per_month
            else:
                sales[d] = sale_amount
        monthly_sales[ticker] = sales
    return monthly_sales


async def get_gross_sales_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute gross sales grid data for all funds.

    Gross sales = monthly delta in cumulative_consideration from shares_issued,
    summed across all share classes and offering types per fund.

    When consideration is rounded (to nearest $100M), uses change in share
    count × prior-period avg NAV per share for a more precise estimate.
    """
    funds = await get_fund_list()
    tickers = [f["ticker"] for f in funds]
    fund_id_map = {f["id"]: f["ticker"] for f in funds}
    fund_ticker_to_id = {f["ticker"]: f["id"] for f in funds}

    async with async_session_factory() as session:
        # Cumulative consideration and shares by (fund_id, as_of_date)
        result = await session.execute(text("""
            SELECT fund_id, as_of_date,
                   SUM(cumulative_consideration) as total_cum,
                   SUM(cumulative_shares) as total_shares
            FROM shares_issued
            WHERE cumulative_consideration IS NOT NULL
            GROUP BY fund_id, as_of_date
            ORDER BY fund_id, as_of_date
        """))
        rows = result.fetchall()

        # Average NAV per share by (fund_id, as_of_date) across share classes
        nav_result = await session.execute(text("""
            SELECT fund_id, as_of_date, AVG(nav_per_share) as avg_nav
            FROM nav_per_share
            WHERE nav_per_share IS NOT NULL
            GROUP BY fund_id, as_of_date
        """))
        nav_rows = nav_result.fetchall()

    # Build NAV lookup: {fund_id: {date: avg_nav}}
    nav_by_fund = defaultdict(dict)
    for fund_id, dt, avg_nav in nav_rows:
        nav_by_fund[fund_id][date.fromisoformat(str(dt))] = float(avg_nav)

    # Group by ticker
    fund_cumulative = defaultdict(list)
    for fund_id, dt, cum, shares in rows:
        ticker = fund_id_map.get(fund_id)
        if ticker:
            fund_cumulative[ticker].append((
                date.fromisoformat(str(dt)), float(cum), float(shares)
            ))

    monthly_sales = _compute_monthly_deltas(fund_cumulative, fund_ticker_to_id, nav_by_fund)

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

    # Compute total sales across all funds for total-level derived metrics
    # Use all available dates (not just filtered range) so Y/Y can find prior-year values
    all_sales_dates = set()
    for sales in monthly_sales.values():
        all_sales_dates.update(sales.keys())
    total_sales = {}
    for d in all_sales_dates:
        s = sum(monthly_sales.get(t, {}).get(d, 0) or 0 for t in tickers)
        if s > 0:
            total_sales[d] = s

    # Compute sub-banks (fund-level)
    yoy_data = {t: compute_yoy_growth(monthly_sales.get(t, {})) for t in tickers}
    total_yoy = compute_yoy_growth(total_sales)

    # For 3M trailing, we need the original monthly data even in quarterly mode
    if period == "quarterly":
        monthly_raw = _compute_monthly_deltas(fund_cumulative, fund_ticker_to_id, nav_by_fund)
        trailing_data = {t: compute_trailing_3m_yoy(monthly_raw.get(t, {})) for t in tickers}
        total_trailing = compute_trailing_3m_yoy(
            {d: sum(monthly_raw.get(t, {}).get(d, 0) or 0 for t in tickers)
             for d in sorted(set().union(*(monthly_raw.get(t, {}).keys() for t in tickers)))}
        )
        # Map to quarter-end dates
        for t in tickers:
            quarterly_trailing = {}
            for d, v in trailing_data[t].items():
                if d in dates:
                    quarterly_trailing[d] = v
            trailing_data[t] = quarterly_trailing
        total_trailing = {d: v for d, v in total_trailing.items() if d in dates}
    else:
        trailing_data = {t: compute_trailing_3m_yoy(monthly_sales.get(t, {})) for t in tickers}
        total_trailing = compute_trailing_3m_yoy(total_sales)

    pct_nav_data = {}
    total_nav_all = {}
    for t in tickers:
        pct_nav_data[t] = pct_of(monthly_sales.get(t, {}), nav_by_ticker.get(t, {}), prior=True)
    # Total % of NAV (t-1) = total sales / prior-period total NAV
    for d in dates:
        nav_sum = sum(_prior_value(nav_by_ticker.get(t, {}), d) or 0 for t in tickers)
        if nav_sum > 0 and d in total_sales:
            total_nav_all[d] = total_sales[d] / nav_sum

    def _total_from_dict(lookup):
        def fn(d, fund_vals):
            return lookup.get(d)
        return fn

    banks = [
        build_bank("Gross Sales", "currency", monthly_sales, tickers, dates),
        build_bank("Y/Y Growth", "percent", yoy_data, tickers, dates, total_fn=_total_from_dict(total_yoy)),
        build_bank("Y/Y Growth - 3M Trailing", "percent", trailing_data, tickers, dates, total_fn=_total_from_dict(total_trailing)),
        build_bank("% of NAV (t-1)", "percent1", pct_nav_data, tickers, dates, total_fn=_total_from_dict(total_nav_all)),
    ]

    return {"funds": tickers, "banks": banks}
