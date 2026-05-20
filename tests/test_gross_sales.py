"""Regression tests for gross sales delta computation.

Run from the repo root with:
    python -m pytest tests/

The bugs these tests guard against:

1. ADS May 2026 was reported as ~$248M instead of ~$46M because Apollo
   reclassified ~$200M of Class I Common Shares from the Primary "Offering"
   bucket into "Private Offering". The old code keyed deltas on
   (share_class, offering_type) and skipped negative deltas, so the −$202M
   Primary leg was dropped while the +$243M Private leg was kept.

2. The negative-delta skip also masks cross-share-class restatements
   (e.g., a balance migrating from Class S into Class I). At the fund
   level the positive and negative must net.
"""

from datetime import date
from collections import defaultdict

from src.api.services.gross_sales import _compute_class_monthly_deltas


def _aggregate_and_compute(rows):
    """Mirror the aggregation that `get_gross_sales_data` does before
    delegating to `_compute_class_monthly_deltas`.

    rows: list of (as_of_date, share_class, offering_type, cum_shares, cum_consid)
    Returns: {month: fund-total sales}
    """
    agg: dict = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
    for d, share_class, _offering_type, shares, cum in rows:
        bucket = agg[share_class][d]
        bucket[0] += float(cum)
        bucket[1] += float(shares)

    totals: dict = defaultdict(float)
    for _share_class, by_date in agg.items():
        data_points = sorted([(d, c, s) for d, (c, s) in by_date.items()])
        class_sales = _compute_class_monthly_deltas(data_points, nav_lookup={})
        for month, amount in class_sales.items():
            totals[month] += amount
    return dict(totals)


def test_ads_may_2026_handles_offering_reclassification():
    """ADS reclassified ~$200M of Class I from Primary to Private between
    the April-1-2026 and May-1-2026 subscription dates. Reported May gross
    sales should reflect only the real ~$46M net increase, not the
    ~$248M ghost increase from looking at Private alone.
    """
    rows = [
        # date, share_class, offering_type, cum_shares, cum_consideration ($)
        (date(2026, 3, 1), "Class I", "Primary",           194_989_713, 4_787_567_307),
        (date(2026, 3, 1), "Class S", "Primary",           124_168_017, 3_051_133_884),
        (date(2026, 3, 1), "Class D", "Primary",             1_543_999,    37_945_681),
        (date(2026, 3, 1), "Class I", "Private Placement", 355_881_240, 8_784_400_318),
        (date(2026, 4, 1), "Class I", "Primary",           195_852_401, 4_808_134_041),
        (date(2026, 4, 1), "Class S", "Primary",           124_659_824, 3_062_882_960),
        (date(2026, 4, 1), "Class D", "Primary",             1_570_782,    38_585_681),
        (date(2026, 4, 1), "Class I", "Private Placement", 356_622_478, 8_802_112_863),
        (date(2026, 5, 1), "Class I", "Primary",           187_634_431, 4_605_844_280),
        (date(2026, 5, 1), "Class S", "Primary",           124_887_122, 3_068_321_314),
        (date(2026, 5, 1), "Class D", "Primary",             1_586_462,    38_960_681),
        (date(2026, 5, 1), "Class I", "Private Placement", 366_526_824, 9_044_732_487),
    ]
    sales = _aggregate_and_compute(rows)

    # Sum of per-class cumulative consideration deltas. These differ by ~$2 (May)
    # and ~$145k (April) from the 8-K's printed grand-total row — small
    # discrepancies that come from Apollo's own arithmetic, not from our parser.
    # The bug we are guarding against is the ~$200M overstatement, so the per-class
    # sum here is what the code should be computing.
    assert sales[date(2026, 5, 1)] == 46_143_217
    assert sales[date(2026, 4, 1)] == 50_668_355
    # Pre-fix value was ~$248M for May; assert we are nowhere near that.
    assert sales[date(2026, 5, 1)] < 100_000_000


def test_negative_delta_across_share_classes_nets_to_zero():
    """If a balance migrates from Class S into Class I (e.g., share-class
    consolidation), Class S goes down and Class I goes up by the same
    amount. The fund-level net must be zero, not just the Class I increase.
    """
    rows = [
        (date(2026, 4, 1), "Class I", "Primary", 10_000_000, 1_000_000_000),
        (date(2026, 4, 1), "Class S", "Primary",  5_000_000,   500_000_000),
        # $100M migrates S → I; no real new sales
        (date(2026, 5, 1), "Class I", "Primary", 14_000_000, 1_100_000_000),
        (date(2026, 5, 1), "Class S", "Primary",  4_000_000,   400_000_000),
    ]
    assert _aggregate_and_compute(rows)[date(2026, 5, 1)] == 0


def test_positive_only_path_unchanged():
    """Sanity check: when nothing odd happens, sum of class deltas equals
    fund-total delta.
    """
    rows = [
        (date(2026, 4, 1), "Class I", "Primary", 10_000_000, 1_000_000_000),
        (date(2026, 4, 1), "Class S", "Primary",  5_000_000,   500_000_000),
        (date(2026, 5, 1), "Class I", "Primary", 10_300_000, 1_030_000_000),
        (date(2026, 5, 1), "Class S", "Primary",  5_150_000,   515_000_000),
    ]
    # $30M + $15M = $45M
    assert _aggregate_and_compute(rows)[date(2026, 5, 1)] == 45_000_000
