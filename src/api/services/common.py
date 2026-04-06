"""Shared helpers for dashboard service computations."""

import calendar
from datetime import date
from decimal import Decimal
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import async_session_factory


def month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def generate_month_ends(start: date, end: date) -> list[date]:
    """Generate month-end dates between start and end (inclusive)."""
    dates = []
    y, m = start.year, start.month
    while True:
        d = month_end(y, m)
        if d > end:
            break
        if d >= start:
            dates.append(d)
        m += 1
        if m > 12:
            m = 1
            y += 1
    return dates


def generate_quarter_ends(start: date, end: date) -> list[date]:
    """Generate quarter-end dates between start and end."""
    qe_months = [3, 6, 9, 12]
    dates = []
    for y in range(start.year, end.year + 1):
        for m in qe_months:
            d = month_end(y, m)
            if start <= d <= end:
                dates.append(d)
    return dates


def quarter_end_for(d: date) -> date:
    """Return the quarter-end date for the quarter containing d."""
    qm = ((d.month - 1) // 3 + 1) * 3
    return month_end(d.year, qm)


def aggregate_quarterly(monthly: dict[date, float | None]) -> dict[date, float | None]:
    """Sum monthly values into quarterly buckets, keyed by quarter-end date."""
    quarterly = defaultdict(lambda: None)
    for d, val in monthly.items():
        if val is None:
            continue
        qe = quarter_end_for(d)
        if quarterly[qe] is None:
            quarterly[qe] = val
        else:
            quarterly[qe] += val
    return dict(quarterly)


def compute_yoy_growth(values: dict[date, float | None]) -> dict[date, float | None]:
    """Compute Y/Y growth. Match each date to 12 months prior (or 4 quarters)."""
    result = {}
    for d, val in values.items():
        if val is None:
            result[d] = None
            continue
        # Find date ~12 months ago
        try:
            prior = d.replace(year=d.year - 1)
        except ValueError:
            prior = date(d.year - 1, d.month, 28)
        # Find closest match in values
        best = None
        for candidate in values:
            if candidate.year == prior.year and candidate.month == prior.month:
                best = candidate
                break
        if best and values[best] is not None and values[best] != 0:
            result[d] = (val - values[best]) / abs(values[best])
        else:
            result[d] = None
    return result


def compute_trailing_3m_yoy(monthly_values: dict[date, float | None]) -> dict[date, float | None]:
    """Compute Y/Y growth on trailing 3-month sums."""
    sorted_dates = sorted(monthly_values.keys())
    date_idx = {d: i for i, d in enumerate(sorted_dates)}
    result = {}

    for d in sorted_dates:
        i = date_idx[d]
        # Sum current + prior 2 months
        window = [sorted_dates[j] for j in range(max(0, i - 2), i + 1)]
        current_sum = sum(monthly_values[w] for w in window if monthly_values[w] is not None)
        if not any(monthly_values[w] is not None for w in window):
            result[d] = None
            continue

        # Same 3-month window a year ago
        try:
            prior_anchor = d.replace(year=d.year - 1)
        except ValueError:
            prior_anchor = date(d.year - 1, d.month, 28)
        # Find the closest date to prior_anchor
        prior_idx = None
        for j, sd in enumerate(sorted_dates):
            if sd.year == prior_anchor.year and sd.month == prior_anchor.month:
                prior_idx = j
                break
        if prior_idx is None:
            result[d] = None
            continue

        prior_window = [sorted_dates[j] for j in range(max(0, prior_idx - 2), prior_idx + 1)]
        prior_sum = sum(monthly_values[w] for w in prior_window if monthly_values[w] is not None)
        if not any(monthly_values[w] is not None for w in prior_window) or prior_sum == 0:
            result[d] = None
            continue

        result[d] = (current_sum - prior_sum) / abs(prior_sum)

    return result


def pct_of(numerator: dict[date, float | None], denominator: dict[date, float | None], prior: bool = False) -> dict[date, float | None]:
    """Compute numerator / denominator for matching dates.

    If prior=True, use the most recent denominator value strictly before
    the numerator date (t-1 semantics).
    """
    result = {}
    for d, val in numerator.items():
        if prior:
            denom = _prior_value(denominator, d)
        else:
            denom = _closest_value(denominator, d)
        if val is not None and denom is not None and denom != 0:
            result[d] = val / denom
        else:
            result[d] = None
    return result


def _prior_value(lookup: dict[date, float | None], target: date) -> float | None:
    """Find the most recent value strictly before target date, within 95 days."""
    best = None
    best_dist = 999
    for d, v in lookup.items():
        if v is not None and d < target:
            dist = (target - d).days
            if dist < best_dist and dist <= 95:
                best = v
                best_dist = dist
    return best


def _closest_value(lookup: dict[date, float | None], target: date) -> float | None:
    """Find the value at target date, or closest date within 45 days."""
    if target in lookup and lookup[target] is not None:
        return lookup[target]
    best = None
    best_dist = 999
    for d, v in lookup.items():
        if v is not None:
            dist = abs((d - target).days)
            if dist < best_dist and dist <= 45:
                best = v
                best_dist = dist
    return best


async def get_fund_list() -> list[dict]:
    """Return active funds ordered by latest total NAV descending."""
    async with async_session_factory() as session:
        result = await session.execute(text("""
            SELECT f.id, f.ticker, f.name, COALESCE(tn.total_nav, 0) as latest_nav
            FROM funds f
            LEFT JOIN total_nav tn ON tn.fund_id = f.id
                AND tn.as_of_date = (SELECT MAX(as_of_date) FROM total_nav WHERE fund_id = f.id)
            WHERE f.active = 1
            ORDER BY latest_nav DESC
        """))
        return [{"id": r[0], "ticker": r[1], "name": r[2]} for r in result.fetchall()]


async def get_total_nav_lookup() -> dict[int, dict[date, float]]:
    """Return {fund_id: {date: total_nav}} for all funds."""
    async with async_session_factory() as session:
        result = await session.execute(text(
            "SELECT fund_id, as_of_date, total_nav FROM total_nav WHERE total_nav IS NOT NULL"
        ))
        lookup = defaultdict(dict)
        for fund_id, dt, val in result.fetchall():
            lookup[fund_id][date.fromisoformat(str(dt))] = float(val)
        return dict(lookup)


async def get_shares_outstanding_lookup() -> dict[int, dict[date, float]]:
    """Return {fund_id: {date: shares_outstanding}} for all funds."""
    async with async_session_factory() as session:
        result = await session.execute(text(
            "SELECT fund_id, as_of_date, total_shares_outstanding FROM shares_outstanding "
            "WHERE total_shares_outstanding IS NOT NULL"
        ))
        lookup = defaultdict(dict)
        for fund_id, dt, val in result.fetchall():
            lookup[fund_id][date.fromisoformat(str(dt))] = float(val)
        return dict(lookup)


def format_date_label(d: date) -> str:
    """Format date for display: 'Mar 2025' for monthly, 'Q1 2025' for quarterly."""
    return d.strftime("%b %Y")


def build_bank(
    name: str,
    fmt: str,
    fund_data: dict[str, dict[date, float | None]],
    tickers: list[str],
    dates: list[date],
    total_fn=None,
) -> dict:
    """Build a bank dict for the API response.

    Args:
        fund_data: {ticker: {date: value}}
        total_fn: callable(date, {ticker: value}) -> total, or 'sum' for simple sum
    """
    rows = []
    for d in dates:
        row = {"date": d.isoformat()}
        total = Decimal("0")
        has_any = False
        for t in tickers:
            val = fund_data.get(t, {}).get(d)
            row[t] = val
            if val is not None:
                total += Decimal(str(val))
                has_any = True

        if total_fn == "sum":
            row["Total"] = float(total) if has_any else None
        elif total_fn is not None:
            row["Total"] = total_fn(d, {t: fund_data.get(t, {}).get(d) for t in tickers})
        else:
            row["Total"] = float(total) if has_any else None

        rows.append(row)
    return {"name": name, "format": fmt, "rows": rows}
