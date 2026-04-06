"""Tab C: Performance computation."""

from datetime import date
from decimal import Decimal
from collections import defaultdict

from sqlalchemy import text

from src.database import async_session_factory
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup,
    generate_month_ends, generate_quarter_ends, build_bank,
)


async def get_performance_data(start: date, end: date, period: str = "monthly") -> dict:
    """Compute performance grid data for all funds.

    Performance = (NAV_t - NAV_{t-1} + dist_t) / NAV_{t-1}
    Uses average across share classes per fund.
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

    # Compute monthly performance per fund using Class I only
    monthly_perf = {}  # {ticker: {date: perf}}
    for fund in funds:
        fid = fund["id"]
        ticker = fund["ticker"]
        nav_data = nav_by_fund[fid]
        dist_data = dist_by_fund[fid]
        dates_sorted = sorted(nav_data.keys())
        perf = {}
        for i in range(1, len(dates_sorted)):
            d = dates_sorted[i]
            d_prev = dates_sorted[i - 1]
            # Use Class I; fall back to average across classes if Class I unavailable
            class_i_keys = [k for k in nav_data[d] if "I" in k and "II" not in k]
            class_i_prev = [k for k in nav_data[d_prev] if "I" in k and "II" not in k]
            if class_i_keys and class_i_prev:
                cls = class_i_keys[0]
                cls_prev = class_i_prev[0]
                nav_t = nav_data[d][cls]
                nav_prev = nav_data[d_prev][cls_prev]
                dist_t = dist_data.get(d, {}).get(cls, 0)
                if nav_prev and nav_prev > 0:
                    perf[d] = (nav_t - nav_prev + dist_t) / nav_prev
            else:
                # Fallback: average across all classes
                classes = set(nav_data[d].keys()) & set(nav_data[d_prev].keys())
                if not classes:
                    continue
                class_perfs = []
                for cls in classes:
                    nav_t = nav_data[d][cls]
                    nav_prev = nav_data[d_prev][cls]
                    dist_t = dist_data.get(d, {}).get(cls, 0)
                    if nav_prev and nav_prev > 0:
                        class_perfs.append((nav_t - nav_prev + dist_t) / nav_prev)
                if class_perfs:
                    perf[d] = sum(class_perfs) / len(class_perfs)
        monthly_perf[ticker] = perf

    # For quarterly: compound monthly returns within each quarter
    if period == "quarterly":
        for ticker in tickers:
            monthly = monthly_perf.get(ticker, {})
            quarterly = {}
            # Group by quarter
            by_quarter = defaultdict(list)
            for d, ret in sorted(monthly.items()):
                qm = ((d.month - 1) // 3 + 1) * 3
                import calendar
                qe = date(d.year, qm, calendar.monthrange(d.year, qm)[1])
                by_quarter[qe].append(ret)
            for qe, rets in by_quarter.items():
                # Compound: (1+r1)(1+r2)(1+r3) - 1
                compound = 1.0
                for r in rets:
                    compound *= (1 + r)
                quarterly[qe] = compound - 1
            monthly_perf[ticker] = quarterly
    all_dates = set()
    for p in monthly_perf.values():
        all_dates.update(p.keys())
    dates = sorted(d for d in all_dates if start <= d <= end)

    # Build total (NAV-weighted average)
    nav_totals = await get_total_nav_lookup()
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_totals.items() if fid in fund_id_map}

    def weighted_avg(d, fund_vals):
        num = 0.0
        denom = 0.0
        for t, val in fund_vals.items():
            if val is None:
                continue
            # Get total NAV for weight — find closest
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

    banks = [
        build_bank("Performance (Class I Total Return)", "percent", monthly_perf, tickers, dates, total_fn=weighted_avg),
    ]

    return {"funds": tickers, "banks": banks}
