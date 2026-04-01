"""Parser for 10-Q and 10-K filings: total NAV and redemption data.

10-Q/10-K filings contain:
- Balance sheet with total net assets (= total NAV)
- Potentially redemption data in "Subsequent Events" or share activity sections
"""

import re
import logging
from datetime import date
from decimal import Decimal

from bs4 import BeautifulSoup

from src.parsers.base import ParsedFiling, TotalNavRecord, RedemptionRecord
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric,
)

logger = logging.getLogger(__name__)


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

    as_of = extract_as_of_date(text) or filing_date

    # Extract total NAV from balance sheet
    nav_records = _parse_total_nav(tables, text, as_of)
    result.total_nav_records.extend(nav_records)

    # Try to extract redemption data from subsequent events or share activity
    redemption_records = _parse_redemptions(tables, text, as_of, form_type)
    result.redemption_records.extend(redemption_records)

    if result.has_data:
        logger.info(
            f"Parsed {form_type}: {len(result.total_nav_records)} total NAV, "
            f"{len(result.redemption_records)} redemptions"
        )

    return result


def _parse_total_nav(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date,
) -> list[TotalNavRecord]:
    """Extract total NAV (net assets) from the balance sheet."""
    records = []

    # Look for the balance sheet / statement of assets table
    balance_keywords = ["net assets", "total net assets", "total assets"]
    table = find_table_with_keywords(tables, balance_keywords, min_matches=1)

    if not table:
        # Try looking for "consolidated statements of assets"
        for t in tables:
            text = " ".join(" ".join(row) for row in t).lower()
            if ("assets" in text and "liabilities" in text) or "net assets" in text:
                table = t
                break

    if not table:
        return records

    # Look for "Total net assets" or "Net assets" row
    net_assets_patterns = [
        r"total\s+net\s+assets",
        r"^net\s+assets$",
        r"net\s+assets\s+attributable",
        r"total\s+equity",
        r"total\s+shareholders.*equity",
        r"members.*capital",
    ]

    for row in table:
        if not row:
            continue
        first_cell = row[0].lower().strip()
        matched = any(re.search(p, first_cell) for p in net_assets_patterns)
        if not matched:
            # Check all cells in the row
            row_text = " ".join(row).lower()
            matched = any(re.search(p, row_text) for p in net_assets_patterns)

        if matched:
            # Find the numeric value (usually the last non-empty numeric cell)
            for cell in reversed(row):
                val = clean_numeric(cell)
                if val is not None and val > Decimal("1000"):
                    # Total NAV is typically in thousands or millions
                    records.append(TotalNavRecord(
                        as_of_date=as_of,
                        total_nav=val,
                    ))
                    return records  # Take the first match

    # Fallback: search the text for "Net assets ... $X"
    pattern = re.compile(
        r"(?:total\s+)?net\s+assets[^$]*?\$\s*([\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    )
    match = pattern.search(full_text)
    if match:
        val = clean_numeric(match.group(1))
        if val is not None and val > Decimal("1000"):
            records.append(TotalNavRecord(as_of_date=as_of, total_nav=val))

    return records


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
