"""Tab B: Redemptions computation."""

import calendar
from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup, get_shares_outstanding_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, compute_yoy_growth, pct_of, build_bank, _closest_value, _prior_value,
    NA, fill_na_after_start, compute_total_with_na,
)


async def get_redemptions_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute redemptions grid data for all funds.

    Redemptions are always shown quarterly — each record is attributed to
    the quarter-end of the quarter in which the tender offer commenced.
    The period parameter is accepted but ignored.
    """
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
            # Snap to quarter-end in case any date isn't already
            qm = ((d.month - 1) // 3 + 1) * 3
            qe = date(d.year, qm, calendar.monthrange(d.year, qm)[1])
            if shares is not None:
                shares_data[ticker][qe] = shares_data[ticker].get(qe, 0) + float(shares)
            if value is not None:
                value_data[ticker][qe] = value_data[ticker].get(qe, 0) + float(value)

    nav_lookup = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_lookup.items() if fid in fund_id_map}
    so_lookup = await get_shares_outstanding_lookup()
    so_by_ticker = {fund_id_map[fid]: sos for fid, sos in so_lookup.items() if fid in fund_id_map}

    # Collect all dates and fill N/A after each fund's series starts
    all_dates = set()
    for d in list(shares_data.values()) + list(value_data.values()):
        all_dates.update(d.keys())
    all_dates_sorted = sorted(all_dates)

    for t in tickers:
        if t in shares_data:
            shares_data[t] = fill_na_after_start(shares_data[t], all_dates_sorted)
        if t in value_data:
            value_data[t] = fill_na_after_start(value_data[t], all_dates_sorted)

    dates = sorted(d for d in all_dates if start <= d <= end)

    # Compute totals using all dates (not just filtered range) so Y/Y can find prior-year
    total_shares = compute_total_with_na(shares_data, tickers, all_dates_sorted)
    total_value = compute_total_with_na(value_data, tickers, all_dates_sorted)

    # Sub-banks
    shares_yoy = {t: compute_yoy_growth(shares_data.get(t, {})) for t in tickers}
    value_yoy = {t: compute_yoy_growth(value_data.get(t, {})) for t in tickers}
    shares_pct_os = {t: pct_of(shares_data.get(t, {}), so_by_ticker.get(t, {}), prior=True) for t in tickers}
    value_pct_nav = {t: pct_of(value_data.get(t, {}), nav_by_ticker.get(t, {}), prior=True) for t in tickers}

    # Total-level derived metrics
    total_shares_yoy = compute_yoy_growth(total_shares)
    total_value_yoy = compute_yoy_growth(total_value)

    total_shares_pct_os = {}
    total_value_pct_nav = {}
    for d in dates:
        ts = total_shares.get(d)
        if ts == NA:
            total_shares_pct_os[d] = NA
        elif ts is not None:
            so_sum = sum(_prior_value(so_by_ticker.get(t, {}), d) or 0 for t in tickers)
            total_shares_pct_os[d] = ts / so_sum if so_sum > 0 else None
        else:
            total_shares_pct_os[d] = None

        tv = total_value.get(d)
        if tv == NA:
            total_value_pct_nav[d] = NA
        elif tv is not None:
            nav_sum = sum(_prior_value(nav_by_ticker.get(t, {}), d) or 0 for t in tickers)
            total_value_pct_nav[d] = tv / nav_sum if nav_sum > 0 else None
        else:
            total_value_pct_nav[d] = None

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
