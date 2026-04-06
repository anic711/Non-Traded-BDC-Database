"""Parser for 10-Q and 10-K filings: total NAV and redemption data.

10-Q/10-K filings contain:
- Balance sheet with total net assets (= total NAV)
- Potentially redemption data in "Subsequent Events" or share activity sections
"""

import re
import logging
from datetime import date
from decimal import Decimal

import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.parsers.base import ParsedFiling, TotalNavRecord, RedemptionRecord, SharesOutstandingRecord
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric,
)

logger = logging.getLogger(__name__)


def _extract_period_end_date(html: str, text: str) -> date | None:
    """Extract the fiscal period end date from 10-Q/10-K filing.

    Uses three strategies in order:
    1. XBRL endDate tag (most reliable for inline XBRL filings)
    2. XBRL context ID date range pattern (e.g., D20230101-20230331)
    3. Text search for "period ended" / "balance sheet as of" (full text)
    """
    # Strategy 1: XBRL <xbrli:endDate> tag — pick the most common period end
    xbrl_dates = re.findall(r"<xbrli:endDate>([\d-]+)</xbrli:endDate>", html[:200000])
    if xbrl_dates:
        from collections import Counter
        most_common = Counter(xbrl_dates).most_common(1)[0][0]
        try:
            return date.fromisoformat(most_common)
        except ValueError:
            pass

    # Strategy 2: XBRL context ID with date range (e.g., _D20230101-20230331)
    ctx_match = re.search(r"_D(\d{8})-(\d{8})", html[:10000])
    if ctx_match:
        end_str = ctx_match.group(2)
        try:
            return date(int(end_str[:4]), int(end_str[4:6]), int(end_str[6:8]))
        except ValueError:
            pass

    # Strategy 3: Text patterns (search full text, not just first 5000 chars)
    from src.parsers.utils import parse_date
    patterns = [
        r"(?:fiscal\s+year|(?:quarterly|annual)\s+period)\s+ended\s+(\w+\s+\d{1,2},?\s+\d{4})",
        r"(?:balance\s+sheet|statement\s+of\s+assets).{0,30}?(?:as\s+of\s+)?(\w+\s+\d{1,2},?\s+\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result = parse_date(match.group(1))
            if result:
                return result
    return None


def parse_10q10k(html: str, filing_date: date, form_type: str) -> ParsedFiling:
    """Parse a 10-Q or 10-K filing for total NAV and redemption data.

    Args:
        html: Raw HTML content of the filing
        filing_date: The filing date from EDGAR
        form_type: '10-Q' or '10-K'

    Returns:
        ParsedFiling with total_nav_records and possibly redemption_records
    """
    result = ParsedFiling()
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    tables = extract_tables(soup)

    if not tables:
        logger.warning(f"No tables found in {form_type} filing")
        return result

    as_of = _extract_period_end_date(html, text) or extract_as_of_date(text) or filing_date

    # Detect scaling factor (many 10-Q/10-K filings report "in thousands")
    scale_factor = Decimal("1")
    text_lower = text.lower()
    if "in thousands" in text_lower:
        scale_factor = Decimal("1000")
    elif "in millions" in text_lower:
        scale_factor = Decimal("1000000")

    # Extract total NAV from balance sheet
    nav_records = _parse_total_nav(tables, text, as_of, scale_factor)
    result.total_nav_records.extend(nav_records)

    # Extract total common shares outstanding from balance sheet
    shares_records = _parse_shares_outstanding(tables, text, as_of, scale_factor)
    result.shares_outstanding_records.extend(shares_records)

    if result.has_data:
        logger.info(
            f"Parsed {form_type}: {len(result.total_nav_records)} total NAV, "
            f"{len(result.shares_outstanding_records)} shares outstanding"
        )

    return result


def _parse_total_nav(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date,
    scale_factor: Decimal = Decimal("1"),
) -> list[TotalNavRecord]:
    """Extract total NAV (net assets) from the consolidated balance sheet.

    Targets the balance sheet table which contains both 'total assets' and
    'total liabilities' rows alongside 'total net assets' / 'total common equity'.
    """
    records = []

    # Strategy 1: Find the consolidated balance sheet table
    # It must have BOTH assets/liabilities structure AND a "total net assets" row
    balance_sheet = None
    for t in tables:
        t_text = " ".join(" ".join(row) for row in t).lower()
        has_total_assets = "total assets" in t_text
        has_liabilities = "liabilities" in t_text
        has_net_assets = ("total net assets" in t_text or
                          "total common equity" in t_text or
                          "total equity" in t_text)
        if has_total_assets and has_liabilities and has_net_assets:
            balance_sheet = t
            break

    # Strategy 2: Look for statement of assets and liabilities
    if not balance_sheet:
        for t in tables:
            t_text = " ".join(" ".join(row) for row in t).lower()
            if "assets" in t_text and "liabilities" in t_text and "net assets" in t_text:
                # Skip if it looks like a NAV per share table
                if "per share" in t_text:
                    continue
                balance_sheet = t
                break

    if not balance_sheet:
        return records

    # Extract "Total net assets" or "Total common equity" value
    # Prefer patterns in this priority order
    target_patterns = [
        r"^total\s+net\s+assets$",
        r"^total\s+common\s+equity$",
        r"^total\s+equity$",
        r"^net\s+assets$",
        r"total\s+net\s+assets",
        r"total\s+common\s+equity",
    ]

    for pattern in target_patterns:
        for row in balance_sheet:
            if not row:
                continue
            first_cell = row[0].lower().strip()
            if re.search(pattern, first_cell):
                # Find the first numeric value (most recent period, usually first data column)
                for cell in row[1:]:
                    val = clean_numeric(cell)
                    if val is not None and abs(val) > Decimal("1000"):
                        records.append(TotalNavRecord(
                            as_of_date=as_of,
                            total_nav=val * scale_factor,
                        ))
                        return records
        if records:
            break

    return records


def _parse_shares_outstanding(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date,
    scale_factor: Decimal = Decimal("1"),
) -> list[SharesOutstandingRecord]:
    """Extract total common shares outstanding from the balance sheet.

    Looks for the common stock row which contains parenthetical text like:
    "(1,920,268,571 and 1,528,574,321 shares issued and outstanding, respectively)"
    or per-class lines with "shares issued and outstanding".
    """
    records = []

    # Strategy 1: Find balance sheet table and look for "shares issued and outstanding"
    balance_sheet = None
    for t in tables:
        t_text = " ".join(" ".join(row) for row in t).lower()
        if ("assets" in t_text and "liabilities" in t_text and
                "shares issued and outstanding" in t_text):
            balance_sheet = t
            break

    if not balance_sheet:
        # Fallback: any table with "shares issued and outstanding"
        for t in tables:
            t_text = " ".join(" ".join(row) for row in t).lower()
            if "shares issued and outstanding" in t_text:
                balance_sheet = t
                break

    if balance_sheet:
        total_shares = _extract_shares_from_balance_sheet(balance_sheet)
        if total_shares is not None:
            # Most BDCs report actual share counts even when dollars are "in thousands".
            # ASIF is an exception — its share counts are also in thousands (XBRL scale=3).
            # Heuristic: if raw value < 1M and scale factor exists, shares are likely scaled.
            if total_shares < Decimal("1000000") and scale_factor > Decimal("1"):
                total_shares = total_shares * scale_factor
            records.append(SharesOutstandingRecord(
                as_of_date=as_of,
                total_shares_outstanding=total_shares,
            ))
            return records

    # Strategy 2: Text-based extraction from full filing text
    # Pattern: "X shares issued and outstanding" or "X and Y shares issued and outstanding"
    # Take the first (most recent period) number
    match = re.search(
        r"([\d,]+)\s+(?:and\s+[\d,]+\s+)?(?:common\s+)?shares?\s+issued\s+and\s+outstanding",
        full_text, re.IGNORECASE,
    )
    if match:
        val = clean_numeric(match.group(1))
        if val is not None and val > 0:
            if val < Decimal("1000000") and scale_factor > Decimal("1"):
                val = val * scale_factor
            records.append(SharesOutstandingRecord(
                as_of_date=as_of,
                total_shares_outstanding=val,
            ))

    return records


def _extract_shares_from_balance_sheet(table: list[list[str]]) -> Decimal | None:
    """Extract total shares outstanding from balance sheet rows.

    Handles two patterns:
    1. Single total: "(1,920,268,571 and 1,528,574,321 shares issued and outstanding)"
       -> returns the first number (current period)
    2. Per-class (OCIC): Multiple "Class X ... N shares issued and outstanding" rows
       -> sums all classes
    """
    total = Decimal("0")
    found_any = False

    for row in table:
        row_text = " ".join(row).lower()
        if "shares issued and outstanding" not in row_text:
            continue

        # Extract share counts from the parenthetical or text
        # Pattern: "N and M shares issued and outstanding" or "N shares issued and outstanding"
        matches = re.findall(
            r"([\d,]+)\s+(?:and\s+[\d,]+\s+)?(?:common\s+)?shares?\s+issued\s+and\s+outstanding",
            " ".join(row), re.IGNORECASE,
        )
        for m in matches:
            val = clean_numeric(m)
            if val is not None and val > 0:
                total += val
                found_any = True

    return total if found_any else None


def _parse_redemptions(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date,
    form_type: str,
) -> list[RedemptionRecord]:
    """Try to extract redemption data from 10-Q/10-K filings."""
    records = []

    # Look for tables with redemption keywords
    redemption_keywords = ["redeem", "repurchase", "tender", "shares accepted"]
    table = find_table_with_keywords(tables, redemption_keywords, min_matches=1)

    if table:
        shares_redeemed = None
        value_redeemed = None

        for row in table:
            row_text = " ".join(row).lower()
            if any(kw in row_text for kw in ["shares redeemed", "shares repurchased",
                                              "shares accepted"]):
                for cell in reversed(row):
                    val = clean_numeric(cell)
                    if val is not None:
                        shares_redeemed = val
                        break
            elif any(kw in row_text for kw in ["aggregate", "consideration", "value redeemed",
                                                "total repurchase"]):
                for cell in reversed(row):
                    val = clean_numeric(cell)
                    if val is not None:
                        value_redeemed = val
                        break

        if shares_redeemed is not None or value_redeemed is not None:
            records.append(RedemptionRecord(
                as_of_date=as_of,
                shares_redeemed=shares_redeemed,
                value_redeemed=value_redeemed,
                source_form_type=form_type,
            ))

    # Also check subsequent events text
    subseq_pattern = re.compile(
        r"subsequent\s+events.*?"
        r"(?:redeem|repurchas|tender).*?"
        r"([\d,]+(?:\.\d+)?)\s+shares.*?"
        r"\$\s*([\d,]+(?:\.\d+)?)",
        re.IGNORECASE | re.DOTALL,
    )
    match = subseq_pattern.search(full_text)
    if match and not records:
        shares_val = clean_numeric(match.group(1))
        value_val = clean_numeric(match.group(2))
        if shares_val or value_val:
            records.append(RedemptionRecord(
                as_of_date=as_of,
                shares_redeemed=shares_val,
                value_redeemed=value_val,
                source_form_type=form_type,
            ))

    return records
