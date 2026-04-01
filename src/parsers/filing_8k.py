"""Parser for 8-K filings: NAV per share, distributions, shares issued.

Non-traded BDC 8-K filings typically report monthly data under Item 7.01
(Regulation FD Disclosure) or Item 8.01 (Other Events). The data appears
in HTML tables with varying structures across different BDCs.

Common table structures:
1. NAV table: share class | NAV per share (as of date)
2. Distribution table: share class | distribution per share | record date | payment date
3. Shares issued table: share class | offering type | cumulative shares | cumulative consideration
"""

import re
import logging
from datetime import date
from decimal import Decimal

from bs4 import BeautifulSoup

from src.parsers.base import (
    ParsedFiling, NavRecord, DistributionRecord, SharesIssuedRecord,
)
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric, normalize_share_class, normalize_offering_type,
)

logger = logging.getLogger(__name__)


def parse_8k(html: str, filing_date: date) -> ParsedFiling:
    """Parse an 8-K filing HTML for NAV, distribution, and shares issued data.

    Args:
        html: Raw HTML content of the 8-K filing
        filing_date: The filing date from EDGAR (used as fallback for as-of date)

    Returns:
        ParsedFiling with extracted records
    """
    result = ParsedFiling()
    soup = BeautifulSoup(html, "lxml")

    # Extract all tables from the filing
    tables = extract_tables(soup)
    if not tables:
        logger.warning("No tables found in 8-K filing")
        return result

    # Try to extract the as-of date from the document text
    text = soup.get_text(separator=" ", strip=True)
    as_of = extract_as_of_date(text)

    # Parse NAV per share
    nav_records = _parse_nav_table(tables, text, as_of, filing_date)
    result.nav_records.extend(nav_records)

    # Parse distributions
    dist_records = _parse_distribution_table(tables, text, as_of, filing_date)
    result.distribution_records.extend(dist_records)

    # Parse shares issued
    shares_records = _parse_shares_issued_table(tables, text, as_of, filing_date)
    result.shares_issued_records.extend(shares_records)

    if result.has_data:
        logger.info(
            f"Parsed 8-K: {len(result.nav_records)} NAV, "
            f"{len(result.distribution_records)} dist, "
            f"{len(result.shares_issued_records)} shares issued"
        )
    else:
        logger.warning("No relevant data found in 8-K filing")

    return result


def _parse_nav_table(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date | None,
    filing_date: date,
) -> list[NavRecord]:
    """Extract NAV per share records from tables."""
    records = []

    # Look for tables with NAV-related keywords
    nav_keywords = ["nav", "net asset value", "per share"]
    table = find_table_with_keywords(tables, nav_keywords, min_matches=1)
    if not table:
        # Try broader search - any table with "Class" and a dollar amount
        for t in tables:
            text = " ".join(" ".join(row) for row in t).lower()
            if "class" in text and ("$" in text or "nav" in text):
                table = t
                break

    if not table:
        return records

    effective_date = as_of or filing_date

    # Strategy 1: Table with "Class X" in rows and NAV values in columns
    # Look for header row to identify column positions
    header_row_idx = _find_header_row(table, ["class", "nav", "per share", "net asset"])
    if header_row_idx is not None:
        nav_col = _find_column(table[header_row_idx], ["nav", "net asset", "per share"])
        for row in table[header_row_idx + 1:]:
            if not row:
                continue
            class_match = _extract_share_class(row)
            if class_match and nav_col is not None and nav_col < len(row):
                nav_val = clean_numeric(row[nav_col])
                records.append(NavRecord(
                    as_of_date=effective_date,
                    share_class=class_match,
                    nav_per_share=nav_val,
                ))
    else:
        # Strategy 2: Scan all rows for share class + numeric pairs
        records.extend(_scan_rows_for_nav(table, effective_date))

    # If we still have no records, try a more aggressive approach
    if not records:
        records.extend(_aggressive_nav_scan(tables, full_text, effective_date))

    return records


def _parse_distribution_table(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date | None,
    filing_date: date,
) -> list[DistributionRecord]:
    """Extract distribution per share records from tables."""
    records = []

    # Must match "distribution" or "dividend" specifically (not just "per share"
    # which would also match NAV tables)
    dist_keywords = ["distribution", "dividend"]
    table = find_table_with_keywords(tables, dist_keywords, min_matches=1)
    if not table:
        return records

    effective_date = as_of or filing_date

    # Look for distribution-specific date in the table context
    dist_date = _extract_date_near_keyword(full_text, ["distribution", "dividend"])
    if dist_date:
        effective_date = dist_date

    header_row_idx = _find_header_row(table, ["class", "distribution", "dividend", "per share"])
    if header_row_idx is not None:
        dist_col = _find_column(table[header_row_idx],
                                ["distribution", "dividend", "per share", "rate"])
        for row in table[header_row_idx + 1:]:
            if not row:
                continue
            class_match = _extract_share_class(row)
            if class_match and dist_col is not None and dist_col < len(row):
                dist_val = clean_numeric(row[dist_col])
                records.append(DistributionRecord(
                    as_of_date=effective_date,
                    share_class=class_match,
                    distribution_per_share=dist_val,
                ))
    else:
        # Scan rows for class + distribution values
        for row in table:
            class_match = _extract_share_class(row)
            if class_match:
                # Find the first numeric value that looks like a distribution
                for cell in row[1:]:
                    val = clean_numeric(cell)
                    if val is not None and val < Decimal("100"):
                        records.append(DistributionRecord(
                            as_of_date=effective_date,
                            share_class=class_match,
                            distribution_per_share=val,
                        ))
                        break

    return records


def _parse_shares_issued_table(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date | None,
    filing_date: date,
) -> list[SharesIssuedRecord]:
    """Extract cumulative shares issued and consideration from tables."""
    records = []

    shares_keywords = ["shares", "issued", "consideration", "offering"]
    table = find_table_with_keywords(tables, shares_keywords, min_matches=2)
    if not table:
        return records

    effective_date = as_of or filing_date

    # These tables tend to have share class and offering type as row identifiers,
    # with cumulative shares and cumulative consideration as columns
    header_row_idx = _find_header_row(table, ["shares", "consideration", "aggregate"])
    if header_row_idx is None:
        return records

    header = table[header_row_idx]
    shares_col = _find_column(header, ["shares", "number"])
    consideration_col = _find_column(header, ["consideration", "aggregate", "value", "proceeds"])

    current_class = None
    for row in table[header_row_idx + 1:]:
        if not row:
            continue

        # Check if this row specifies a share class
        class_match = _extract_share_class(row)
        if class_match:
            current_class = class_match

        # Check for offering type
        offering_match = _extract_offering_type(row)
        if offering_match and current_class:
            shares_val = None
            consid_val = None
            if shares_col is not None and shares_col < len(row):
                shares_val = clean_numeric(row[shares_col])
            if consideration_col is not None and consideration_col < len(row):
                consid_val = clean_numeric(row[consideration_col])

            if shares_val is not None or consid_val is not None:
                records.append(SharesIssuedRecord(
                    as_of_date=effective_date,
                    share_class=current_class,
                    offering_type=offering_match,
                    cumulative_shares=shares_val,
                    cumulative_consideration=consid_val,
                ))

    # If we couldn't parse with the structured approach, try row-by-row
    if not records:
        records.extend(_scan_rows_for_shares_issued(table, effective_date))

    return records


# --- Helper functions ---

def _find_header_row(table: list[list[str]], keywords: list[str]) -> int | None:
    """Find the index of the header row in a table."""
    keywords_lower = [k.lower() for k in keywords]
    for i, row in enumerate(table[:5]):  # Check first 5 rows
        row_text = " ".join(row).lower()
        if any(kw in row_text for kw in keywords_lower):
            return i
    return None


def _find_column(header_row: list[str], keywords: list[str]) -> int | None:
    """Find the column index matching any of the keywords."""
    keywords_lower = [k.lower() for k in keywords]
    for i, cell in enumerate(header_row):
        cell_lower = cell.lower()
        if any(kw in cell_lower for kw in keywords_lower):
            return i
    return None


def _extract_share_class(row: list[str]) -> str | None:
    """Extract a share class name from a table row."""
    for cell in row:
        match = re.search(r"[Cc]lass\s+([A-Z])", cell, re.IGNORECASE)
        if match:
            return normalize_share_class(cell)
    return None


def _extract_offering_type(row: list[str]) -> str | None:
    """Extract an offering type from a table row."""
    for cell in row:
        cell_lower = cell.lower()
        if any(kw in cell_lower for kw in
               ["primary", "drip", "reinvestment", "private placement", "offering"]):
            return normalize_offering_type(cell)
    return None


def _scan_rows_for_nav(table: list[list[str]], effective_date: date) -> list[NavRecord]:
    """Fallback: scan all rows looking for share class + NAV value patterns."""
    records = []
    for row in table:
        class_match = _extract_share_class(row)
        if not class_match:
            continue
        # Look for dollar values in the row (NAV per share is typically $20-$30)
        for cell in row:
            val = clean_numeric(cell)
            if val is not None and Decimal("1") <= val <= Decimal("1000"):
                records.append(NavRecord(
                    as_of_date=effective_date,
                    share_class=class_match,
                    nav_per_share=val,
                ))
                break
    return records


def _aggressive_nav_scan(
    tables: list[list[list[str]]],
    full_text: str,
    effective_date: date,
) -> list[NavRecord]:
    """Last resort: look for NAV per share patterns in any table or text."""
    records = []
    # Search the full text for patterns like "Class I ... $25.12"
    pattern = re.compile(
        r"(Class\s+[A-Z])\s+(?:shares?\s+)?.*?"
        r"\$\s*([\d,]+\.?\d*)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(full_text):
        share_class = normalize_share_class(match.group(1))
        val = clean_numeric(match.group(2))
        if val is not None and Decimal("1") <= val <= Decimal("1000"):
            # Avoid duplicates
            if not any(r.share_class == share_class for r in records):
                records.append(NavRecord(
                    as_of_date=effective_date,
                    share_class=share_class,
                    nav_per_share=val,
                ))
    return records


def _scan_rows_for_shares_issued(
    table: list[list[str]],
    effective_date: date,
) -> list[SharesIssuedRecord]:
    """Fallback scan for shares issued data."""
    records = []
    current_class = None
    for row in table:
        class_match = _extract_share_class(row)
        if class_match:
            current_class = class_match
        offering_match = _extract_offering_type(row)
        if offering_match and current_class:
            # Collect all numeric values from the row
            numerics = [clean_numeric(cell) for cell in row if clean_numeric(cell) is not None]
            shares_val = numerics[0] if len(numerics) > 0 else None
            consid_val = numerics[1] if len(numerics) > 1 else None
            records.append(SharesIssuedRecord(
                as_of_date=effective_date,
                share_class=current_class,
                offering_type=offering_match,
                cumulative_shares=shares_val,
                cumulative_consideration=consid_val,
            ))
    return records


def _extract_date_near_keyword(text: str, keywords: list[str]) -> date | None:
    """Look for a date near specific keywords in the text."""
    from src.parsers.utils import parse_date
    for kw in keywords:
        pattern = re.compile(
            rf"{kw}.*?(\w+\s+\d{{1,2}},?\s+\d{{4}})",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(text[:3000])  # Search first 3000 chars
        if match:
            result = parse_date(match.group(1))
            if result:
                return result
    return None
