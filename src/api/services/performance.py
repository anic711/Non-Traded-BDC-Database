"""Tab C: Performance computation."""

import calendar
from datetime import date
from decimal import Decimal
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup,
    generate_month_ends, generate_quarter_ends, build_bank,
    NA, fill_na_after_start,
)


def _compound_quarterly(monthly: dict[date, float]) -> dict[date, float]:
    """Compound monthly returns into quarterly returns."""
    by_quarter = defaultdict(list)
    for d, ret in sorted(monthly.items()):
        qm = ((d.month - 1) // 3 + 1) * 3
        qe = date(d.year, qm, calendar.monthrange(d.year, qm)[1])
        by_quarter[qe].append(ret)
    quarterly = {}
    for qe, rets in by_quarter.items():
        if any(r == NA for r in rets):
            quarterly[qe] = NA
            continue
        compound = 1.0
        for r in rets:
            compound *= (1 + r)
        quarterly[qe] = compound - 1
    return quarterly


async def get_performance_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute performance grid data for all funds.

    Total Return = (NAV_t - NAV_{t-1} + dist_t) / NAV_{t-1}
    Price Return = (NAV_t - NAV_{t-1}) / NAV_{t-1}
    Income Return = dist_t / NAV_{t-1}
    Uses Class I share class per fund.
    Total column: NAV-weighted average across funds.
    """
    funds = await get_fund_list()
    tickers = [f["ticker"] for f in funds]
    fund_id_map = {f["id"]: f["ticker"] for f in funds}

    # Load NAV per share by fund
    async with async_session_factory() as session:
        nav_result = await session.execute(text("""
            SELECT fund_id, as_of_date, share_class, nav_per_share
            FROM nav_per_share
            WHERE nav_per_share IS NOT NULL
            ORDER BY fund_id, as_of_date
        """))
        nav_rows = nav_result.fetchall()

        dist_result = await session.execute(text("""
            SELECT fund_id, as_of_date, share_class, distribution_per_share
            FROM distributions
            WHERE distribution_per_share IS NOT NULL
            ORDER BY fund_id, as_of_date
        """))
        dist_rows = dist_result.fetchall()

    # Build {fund_id: {date: {class: nav}}}
    nav_by_fund = defaultdict(lambda: defaultdict(dict))
    for fund_id, dt, cls, val in nav_rows:
        nav_by_fund[fund_id][date.fromisoformat(str(dt))][cls] = float(val)

    dist_by_fund = defaultdict(lambda: defaultdict(dict))
    for fund_id, dt, cls, val in dist_rows:
        dist_by_fund[fund_id][date.fromisoformat(str(dt))][cls] = float(val)

    # Compute monthly returns per fund using Class I
    total_return = {}
    price_return = {}
    income_return = {}

    for fund in funds:
        fid = fund["id"]
        ticker = fund["ticker"]
        nav_data = nav_by_fund[fid]
        dist_data = dist_by_fund[fid]
        dates_sorted = sorted(nav_data.keys())
        tr, pr, ir = {}, {}, {}

        for i in range(1, len(dates_sorted)):
            d = dates_sorted[i]
            d_prev = dates_sorted[i - 1]

            # Find Class I keys
            class_i = [k for k in nav_data[d] if "I" in k and "II" not in k]
            class_i_prev = [k for k in nav_data[d_prev] if "I" in k and "II" not in k]

            if class_i and class_i_prev:
                cls = class_i[0]
                cls_prev = class_i_prev[0]
                nav_t = nav_data[d][cls]
                nav_prev = nav_data[d_prev][cls_prev]
                dist_t = dist_data.get(d, {}).get(cls, 0)
                if nav_prev and nav_prev > 0:
                    tr[d] = (nav_t - nav_prev + dist_t) / nav_prev
                    pr[d] = (nav_t - nav_prev) / nav_prev
                    ir[d] = dist_t / nav_prev
            else:
                # Fallback: average across all classes
                classes = set(nav_data[d].keys()) & set(nav_data[d_prev].keys())
                if not classes:
                    continue
                tr_vals, pr_vals, ir_vals = [], [], []
                for cls in classes:
                    nav_t = nav_data[d][cls]
                    nav_prev = nav_data[d_prev][cls]
                    dist_t = dist_data.get(d, {}).get(cls, 0)
                    if nav_prev and nav_prev > 0:
                        tr_vals.append((nav_t - nav_prev + dist_t) / nav_prev)
                        pr_vals.append((nav_t - nav_prev) / nav_prev)
                        ir_vals.append(dist_t / nav_prev)
                if tr_vals:
                    tr[d] = sum(tr_vals) / len(tr_vals)
                    pr[d] = sum(pr_vals) / len(pr_vals)
                    ir[d] = sum(ir_vals) / len(ir_vals)

        total_return[ticker] = tr
        price_return[ticker] = pr
        income_return[ticker] = ir

    # Fill N/A on monthly data BEFORE quarterly compounding so that a quarter
    # with any missing month is marked N/A.
    all_monthly = set()
    for p in total_return.values():
        all_monthly.update(p.keys())
    all_monthly_sorted = sorted(all_monthly)
    for ticker in tickers:
        if ticker in total_return:
            total_return[ticker] = fill_na_after_start(total_return[ticker], all_monthly_sorted, any_value=True)
        if ticker in price_return:
            price_return[ticker] = fill_na_after_start(price_return[ticker], all_monthly_sorted, any_value=True)
        if ticker in income_return:
            income_return[ticker] = fill_na_after_start(income_return[ticker], all_monthly_sorted, any_value=True)

    # For quarterly: compound monthly returns within each quarter
    if period == "quarterly":
        for ticker in tickers:
            total_return[ticker] = _compound_quarterly(total_return.get(ticker, {}))
            price_return[ticker] = _compound_quarterly(price_return.get(ticker, {}))
            income_return[ticker] = _compound_quarterly(income_return.get(ticker, {}))

    all_dates = set()
    for p in total_return.values():
        all_dates.update(p.keys())
    all_dates_sorted = sorted(all_dates)

    dates = sorted(d for d in all_dates if start <= d <= end)

    # Build total (NAV-weighted average)
    nav_totals = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_totals.items() if fid in fund_id_map}

    def weighted_avg_fn(data_dict):
        def weighted_avg(d, fund_vals):
            # N/A propagation is handled by build_bank (has_na check),
            # but also guard here in case total_fn is called directly.
            if any(v == NA for v in fund_vals.values()):
                return NA
            num = 0.0
            denom = 0.0
            for t, val in fund_vals.items():
                if val is None:
                    continue
                nav_dict = nav_by_ticker.get(t, {})
                weight = None
                for nd in sorted(nav_dict.keys(), key=lambda x: abs((x - d).days)):
                    if abs((nd - d).days) <= 95:
                        weight = nav_dict[nd]
                        break
                if weight and weight > 0:
                    num += val * weight
                    denom += weight
            return num / denom if denom > 0 else None
        return weighted_avg

    banks = [
        build_bank("Total Return", "percent1", total_return, tickers, dates,
                    total_fn=weighted_avg_fn(total_return), subtitle="Class I shareholders"),
        build_bank("Price Return", "percent1", price_return, tickers, dates,
                    total_fn=weighted_avg_fn(price_return), subtitle="Class I shareholders"),
        build_bank("Income Return", "percent1", income_return, tickers, dates,
                    total_fn=weighted_avg_fn(income_return), subtitle="Class I shareholders"),
    ]

    return {"funds": tickers, "banks": banks}
