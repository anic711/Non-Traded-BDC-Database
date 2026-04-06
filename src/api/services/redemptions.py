"""Tab B: Redemptions computation."""

from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup, get_shares_outstanding_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, compute_yoy_growth, pct_of, build_bank, _closest_value, _prior_value,
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

    # Compute totals for derived metrics
    total_shares = {d: sum(shares_data.get(t, {}).get(d, 0) or 0 for t in tickers) for d in dates}
    total_shares = {d: v for d, v in total_shares.items() if v > 0}
    total_value = {d: sum(value_data.get(t, {}).get(d, 0) or 0 for t in tickers) for d in dates}
    total_value = {d: v for d, v in total_value.items() if v > 0}

    # Sub-banks
    shares_yoy = {t: compute_yoy_growth(shares_data.get(t, {})) for t in tickers}
    value_yoy = {t: compute_yoy_growth(value_data.get(t, {})) for t in tickers}
    shares_pct_os = {t: pct_of(shares_data.get(t, {}), so_by_ticker.get(t, {}), prior=True) for t in tickers}
    value_pct_nav = {t: pct_of(value_data.get(t, {}), nav_by_ticker.get(t, {}), prior=True) for t in tickers}

    # Total-level derived metrics
    total_shares_yoy = compute_yoy_growth(total_shares)
    total_value_yoy = compute_yoy_growth(total_value)
    total_so = {d: sum(_prior_value(so_by_ticker.get(t, {}), d) or 0 for t in tickers) for d in dates}
    total_shares_pct_os = {d: total_shares[d] / total_so[d] if total_so.get(d) and d in total_shares else None for d in dates}
    total_nav_sum = {d: sum(_prior_value(nav_by_ticker.get(t, {}), d) or 0 for t in tickers) for d in dates}
    total_value_pct_nav = {d: total_value[d] / total_nav_sum[d] if total_nav_sum.get(d) and d in total_value else None for d in dates}

    def _total_from(lookup):
        def fn(d, fund_vals):
            return lookup.get(d)
        return fn

    banks = [
        build_bank("Shares Redeemed", "number", dict(shares_data), tickers, dates),
        build_bank("Y/Y Growth (Shares)", "percent", shares_yoy, tickers, dates, total_fn=_total_from(total_shares_yoy)),
        build_bank("% of Shares O/S (t-1)", "percent1", shares_pct_os, tickers, dates, total_fn=_total_from(total_shares_pct_os)),
        build_bank("Value of Shares Redeemed", "currency", dict(value_data), tickers, dates),
        build_bank("Y/Y Growth (Value)", "percent", value_yoy, tickers, dates, total_fn=_total_from(total_value_yoy)),
        build_bank("% of NAV (t-1)", "percent1", value_pct_nav, tickers, dates, total_fn=_total_from(total_value_pct_nav)),
    ]

    return {"funds": tickers, "banks": banks}
