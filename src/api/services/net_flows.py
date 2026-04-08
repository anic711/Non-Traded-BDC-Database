"""Tab E: Net Flows computation (quarterly gross sales - quarterly redemptions)."""

from datetime import date

from src.api.services.gross_sales import get_gross_sales_data
from src.api.services.redemptions import get_redemptions_data
from src.api.services.common import (
    get_fund_list, get_total_nav_lookup,
    compute_yoy_growth, pct_of, build_bank, _prior_value,
    NA, fill_na_after_start, compute_total_with_na,
)


def _yoy_growth_signed(values: dict) -> dict:
    """Y/Y growth that returns N/A when the sign flips between periods."""
    raw = compute_yoy_growth(values)
    for d, g in raw.items():
        if g is None or g == NA:
            continue
        val = values.get(d)
        # Find prior year value
        try:
            prior_d = d.replace(year=d.year - 1)
        except ValueError:
            prior_d = date(d.year - 1, d.month, 28)
        prior_val = None
        for c in values:
            if c.year == prior_d.year and c.month == prior_d.month:
                prior_val = values[c]
                break
        if prior_val is None or prior_val == NA or val is None or val == NA:
            continue
        # Sign change → N/A
        if (val >= 0) != (prior_val >= 0):
            raw[d] = NA
    return raw


async def get_net_flows_data(start: date, end: date, period: str = "quarterly") -> dict:
    """Compute net flows = quarterly gross sales + quarterly redemptions (negative).

    Shows N/A if either gross sales or redemptions is N/A for a given fund/date.
    Always quarterly.
    """
    # Fetch quarterly gross sales and redemptions using a wide date range
    # so Y/Y growth can look back 4 quarters beyond the display range.
    wide_start = date(start.year - 2, 1, 1)
    sales_data = await get_gross_sales_data(wide_start, end, "quarterly")
    redemptions_data = await get_redemptions_data(wide_start, end, "quarterly")

    funds = sales_data.get("funds", [])
    tickers = list(funds)

    # Extract the raw Gross Sales and Value of Shares Redeemed banks
    sales_bank = next((b for b in sales_data["banks"] if b["name"] == "Gross Sales"), None)
    redemptions_bank = next((b for b in redemptions_data["banks"] if b["name"] == "Value of Shares Redeemed"), None)

    if not sales_bank or not redemptions_bank:
        return {"funds": tickers, "banks": []}

    # Build {ticker: {date: value}} for sales and redemptions
    sales_by_fund = {}
    for t in tickers:
        sales_by_fund[t] = {}
        for row in sales_bank["rows"]:
            d = date.fromisoformat(row["date"])
            sales_by_fund[t][d] = row.get(t)

    red_by_fund = {}
    for t in tickers:
        red_by_fund[t] = {}
        for row in redemptions_bank["rows"]:
            d = date.fromisoformat(row["date"])
            red_by_fund[t][d] = row.get(t)

    # Compute net flows = gross sales - redemptions
    # N/A if either input is N/A or None
    net_flows = {}
    for t in tickers:
        nf = {}
        all_dates = sorted(set(sales_by_fund[t].keys()) | set(red_by_fund[t].keys()))
        for d in all_dates:
            s = sales_by_fund[t].get(d)
            r = red_by_fund[t].get(d)
            if s == NA or r == NA:
                nf[d] = NA
            elif s is not None and r is not None:
                nf[d] = s - r
            elif s is not None:
                # Have sales but no redemption data yet — can't compute net
                nf[d] = NA
            elif r is not None:
                nf[d] = NA
        net_flows[t] = nf

    # Collect all dates
    all_dates_set = set()
    for nf in net_flows.values():
        all_dates_set.update(nf.keys())
    all_dates_sorted = sorted(all_dates_set)

    dates = sorted(d for d in all_dates_set if start <= d <= end)

    # Total net flows
    total_net = compute_total_with_na(net_flows, tickers, all_dates_sorted)

    # Y/Y growth (N/A on sign change since growth % is meaningless)
    yoy_data = {t: _yoy_growth_signed(net_flows.get(t, {})) for t in tickers}
    total_yoy = _yoy_growth_signed(total_net)

    # % of NAV (t-1)
    nav_lookup = await get_total_nav_lookup()
    fund_list = await get_fund_list()
    fund_id_map = {f["id"]: f["ticker"] for f in fund_list}
    nav_by_ticker = {fund_id_map[fid]: navs for fid, navs in nav_lookup.items() if fid in fund_id_map}

    pct_nav_data = {t: pct_of(net_flows.get(t, {}), nav_by_ticker.get(t, {}), prior=True) for t in tickers}

    total_pct_nav = {}
    for d in dates:
        tn = total_net.get(d)
        if tn == NA:
            total_pct_nav[d] = NA
        elif tn is not None:
            nav_sum = sum(_prior_value(nav_by_ticker.get(t, {}), d) or 0 for t in tickers)
            total_pct_nav[d] = tn / nav_sum if nav_sum > 0 else None

    def _total_from(lookup):
        def fn(d, fund_vals):
            return lookup.get(d)
        return fn

    banks = [
        build_bank("Net Flows", "currency", net_flows, tickers, dates),
        build_bank("Y/Y Growth", "percent", yoy_data, tickers, dates, total_fn=_total_from(total_yoy)),
        build_bank("% of NAV (t-1)", "percent1", pct_nav_data, tickers, dates, total_fn=_total_from(total_pct_nav)),
    ]

    return {"funds": tickers, "banks": banks}
