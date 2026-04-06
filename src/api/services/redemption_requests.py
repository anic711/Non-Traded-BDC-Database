"""Tab D: Redemption Requests computation."""

import calendar
from datetime import date
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup, get_shares_outstanding_lookup,
    generate_month_ends, generate_quarter_ends,
    aggregate_quarterly, pct_of, build_bank, _closest_value, _prior_value,
    NA, fill_na_after_start, compute_total_with_na,
)


async def get_redemption_requests_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute redemption requests grid data for all funds.

    Redemption requests are always shown quarterly — each record is attributed
    to the quarter-end of the quarter in which the tender offer commenced.
    The period parameter is accepted but ignored.
    """
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
        # Snap to quarter-end
        qm = ((d.month - 1) // 3 + 1) * 3
        qe = date(d.year, qm, calendar.monthrange(d.year, qm)[1])

        if tendered is not None:
            t_val = float(tendered)
            shares_tendered[ticker][qe] = shares_tendered[ticker].get(qe, 0) + t_val

            # Compute value of shares tendered = shares_tendered × avg NAV
            avg_nav = _closest_value(avg_nav_by_fund.get(fund_id, {}), d)
            if avg_nav:
                value_tendered[ticker][qe] = value_tendered[ticker].get(qe, 0) + t_val * avg_nav

            # % fulfilled = shares_redeemed / shares_tendered
            if redeemed is not None and t_val > 0:
                pct_fulfilled[ticker][qe] = float(redeemed) / t_val

    nav_lookup = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_lookup.items() if fid in fund_id_map}
    so_lookup = await get_shares_outstanding_lookup()
    so_by_ticker = {fund_id_map[fid]: sos for fid, sos in so_lookup.items() if fid in fund_id_map}

    # Collect all dates and fill N/A after each fund's series starts
    all_dates = set()
    for d in list(shares_tendered.values()) + list(value_tendered.values()):
        all_dates.update(d.keys())
    all_dates_sorted = sorted(all_dates)

    for t in tickers:
        if t in shares_tendered:
            shares_tendered[t] = fill_na_after_start(shares_tendered[t], all_dates_sorted)
        if t in value_tendered:
            value_tendered[t] = fill_na_after_start(value_tendered[t], all_dates_sorted)
        if t in pct_fulfilled:
            pct_fulfilled[t] = fill_na_after_start(pct_fulfilled[t], all_dates_sorted, any_value=True)

    dates = sorted(d for d in all_dates if start <= d <= end)

    shares_pct_os = {t: pct_of(shares_tendered.get(t, {}), so_by_ticker.get(t, {}), prior=True) for t in tickers}
    value_pct_nav = {t: pct_of(value_tendered.get(t, {}), nav_by_ticker.get(t, {}), prior=True) for t in tickers}

    # Total-level derived metrics
    total_shares = compute_total_with_na(shares_tendered, tickers, all_dates_sorted)
    total_value = compute_total_with_na(value_tendered, tickers, all_dates_sorted)

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

    # Total % fulfilled = total shares redeemed / total shares tendered
    total_redeemed = {}
    for d in dates:
        ts = total_shares.get(d)
        if ts == NA:
            total_redeemed[d] = NA
            continue
        redeemed_sum = 0
        tendered_sum = 0
        for t in tickers:
            ten = shares_tendered.get(t, {}).get(d)
            ful = pct_fulfilled.get(t, {}).get(d)
            if ten == NA or ful == NA:
                total_redeemed[d] = NA
                break
            if ten and ful is not None:
                redeemed_sum += ten * ful
                tendered_sum += ten
        else:
            total_redeemed[d] = redeemed_sum / tendered_sum if tendered_sum > 0 else None

    def _total_from(lookup):
        def fn(d, fund_vals):
            return lookup.get(d)
        return fn

    banks = [
        build_bank("Shares Tendered", "number", dict(shares_tendered), tickers, dates),
        build_bank("% of Shares O/S (t-1)", "percent1", shares_pct_os, tickers, dates, total_fn=_total_from(total_shares_pct_os)),
        build_bank("Value of Shares Tendered", "currency", dict(value_tendered), tickers, dates),
        build_bank("% of NAV (t-1)", "percent1", value_pct_nav, tickers, dates, total_fn=_total_from(total_value_pct_nav)),
        build_bank("% Fulfilled", "percent1", dict(pct_fulfilled), tickers, dates, total_fn=_total_from(total_redeemed)),
    ]

    return {"funds": tickers, "banks": banks}
