"""Regression tests for shareholder-letter tender parsing (3Q26 letters).

Sentences are trimmed from the actual filings:
- BCRED: SC TO-I/A exhibit (a)(1)(vii), filed 2026-09-03
- HLEND: 8-K exhibit, filed 2026-09-11
- ADS: 8-K primary document, filed 2026-09-22
"""

from datetime import date
from decimal import Decimal

from src.parsers.filing_8k import parse_8k_exhibit_for_redemptions


def _parse(body: str, filed: date):
    recs = parse_8k_exhibit_for_redemptions(f"<html><body><p>{body}</p></body></html>", filed)
    assert len(recs) == 1
    return recs[0]


def test_bcred_q3_requests_representing_pct_and_prior_quarter_mention():
    rec = _parse(
        "In Q2, BCRED fulfilled approximately half of the $4.5 billion requested for "
        "repurchase, leaving a backlog of $2.3 billion in unfulfilled requests. Overall "
        "repurchase requests in Q3 were an estimated $4.3 billion, 4 representing "
        "approximately 10% of shares outstanding. 5 BCRED will fulfill repurchase "
        "requests representing 5% of shares outstanding.",
        date(2026, 9, 3),
    )
    assert rec.as_of_date == date(2026, 9, 30)
    assert rec.pct_tendered_of_os == Decimal("10")
    # $4.5B is Q2's requested amount, not Q3 repurchases
    assert rec.value_redeemed is None


def test_bcred_q2_requests_are_pct():
    rec = _parse(
        "In Q1, the Fund fulfilled 100% of repurchase requests. In Q2, repurchase "
        "requests are approximately 10% of shares outstanding, 7 and as designed, BCRED "
        "will fulfill repurchase requests representing 5% of shares outstanding.",
        date(2026, 6, 4),
    )
    assert rec.as_of_date == date(2026, 6, 30)
    assert rec.pct_tendered_of_os == Decimal("10")


def test_hlend_quarter_without_year_not_confused_with_share_count_date():
    rec = _parse(
        "During the third quarter, HLEND received repurchase requests totaling "
        "approximately 11.5% of shares outstanding as of June 30, 2026, down from "
        "approximately 13.3% in the second quarter. HLEND will repurchase 5.0% of "
        "shares outstanding as of June 30, 2026, or approximately $600 million.",
        date(2026, 9, 11),
    )
    assert rec.as_of_date == date(2026, 9, 30)
    assert rec.pct_tendered_of_os == Decimal("11.5")
    assert rec.value_redeemed == Decimal("600000000")


def test_ads_requests_to_repurchase():
    rec = _parse(
        "During the third quarter of 2026, ADS received $0.2 billion in gross "
        "subscriptions. For the third quarter, ADS also received shareholder requests "
        "to repurchase approximately 14.7% of shares outstanding 8 . ADS will honor "
        "repurchase requests for 5% of shares outstanding, which we estimate represents "
        "approximately $0.7 billion of gross outflows for the third quarter.",
        date(2026, 9, 22),
    )
    assert rec.as_of_date == date(2026, 9, 30)
    assert rec.pct_tendered_of_os == Decimal("14.7")
    assert rec.value_redeemed == Decimal("700000000")


def test_fourth_quarter_letter_filed_in_january_uses_prior_year():
    rec = _parse(
        "During the fourth quarter, the Fund received repurchase requests totaling "
        "approximately 8.0% of shares outstanding.",
        date(2027, 1, 12),
    )
    assert rec.as_of_date == date(2026, 12, 31)
