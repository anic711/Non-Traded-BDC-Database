"""Parser for SC TO-I and SC TO-I/A filings: share redemption data.

SC TO-I/A (amended issuer tender offer statement) filings report:
- Shares accepted for purchase / redeemed
- Aggregate consideration paid (value of shares redeemed)
- Offer dates, record dates, payment dates
"""

import re
import logging
from datetime import date
from decimal import Decimal

from bs4 import BeautifulSoup

from src.parsers.base import ParsedFiling, RedemptionRecord
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric,
)

logger = logging.getLogger(__name__)


def parse_sctoi(html: str, filing_date: date) -> ParsedFiling:
    """Parse an SC TO-I or SC TO-I/A filing for redemption data.

    Args:
        html: Raw HTML content of the filing
        filing_date: The filing date from EDGAR

    Returns:
        ParsedFiling with redemption_records
    """
    result = ParsedFiling()
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    tables = extract_tables(soup)

    as_of = extract_as_of_date(text) or filing_date

    # Try table-based extraction first
    redemption = _parse_from_tables(tables, text, as_of)
    if redemption:
        result.redemption_records.append(redemption)
    else:
        # Fall back to text-based extraction
        redemption = _parse_from_text(text, as_of)
        if redemption:
            result.redemption_records.append(redemption)

    if result.has_data:
        logger.info(f"Parsed SC TO-I/A: {len(result.redemption_records)} redemptions")
    else:
        logger.warning("No redemption data found in SC TO-I/A filing")

    return result


def _parse_from_tables(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date,
) -> RedemptionRecord | None:
    """Extract redemption data from tables in SC TO-I/A filing."""
    shares_redeemed = None
    value_redeemed = None

    # Look for tables with tender offer keywords
    tender_keywords = ["shares", "tender", "repurchase", "purchase", "accepted"]
    table = find_table_with_keywords(tables, tender_keywords, min_matches=1)

    if not table:
        return None

    for row in table:
        row_text = " ".join(row).lower()

        # Look for shares redeemed/accepted
        if any(kw in row_text for kw in ["shares accepted", "shares repurchased",
                                          "shares tendered", "number of shares",
                                          "shares purchased"]):
            for cell in reversed(row):
                val = clean_numeric(cell)
                if val is not None and val > 0:
                    shares_redeemed = val
                    break

        # Look for value/consideration
        if any(kw in row_text for kw in ["aggregate", "consideration", "purchase price",
                                          "total cost", "amount paid"]):
            for cell in reversed(row):
                val = clean_numeric(cell)
                if val is not None and val > 0:
                    value_redeemed = val
                    break

    if shares_redeemed is not None or value_redeemed is not None:
        return RedemptionRecord(
            as_of_date=as_of,
            shares_redeemed=shares_redeemed,
            value_redeemed=value_redeemed,
            source_form_type="SC TO-I/A",
        )
    return None


def _parse_from_text(text: str, as_of: date) -> RedemptionRecord | None:
    """Extract redemption data from the filing text when tables don't work."""
    shares_redeemed = None
    value_redeemed = None

    # Pattern: "X shares were accepted for purchase" or similar
    shares_patterns = [
        r"([\d,]+(?:\.\d+)?)\s+shares\s+(?:were\s+)?(?:accepted|repurchased|tendered|purchased)",
        r"(?:accepted|repurchased|purchased)\s+([\d,]+(?:\.\d+)?)\s+shares",
        r"total\s+(?:of\s+)?([\d,]+(?:\.\d+)?)\s+shares",
    ]
    for pattern in shares_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            val = clean_numeric(match.group(1))
            if val is not None and val > 0:
                shares_redeemed = val
                break

    # Pattern: "aggregate consideration of/was/paid $X" or "total purchase price of $X"
    value_patterns = [
        r"(?:aggregate|total)\s+consideration\s+(?:paid\s+)?(?:was\s+|of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"(?:aggregate|total)\s+(?:purchase\s+price|cost)\s+(?:of\s+|was\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"\$\s*([\d,]+(?:\.\d+)?)\s+(?:in\s+)?(?:aggregate|total)\s+(?:consideration|purchase)",
        r"(?:aggregate|total)\s+(?:repurchase|redemption)\s+(?:amount|price)\s+(?:of\s+|was\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"consideration\s+(?:paid\s+)?(?:was\s+|of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
    ]
    for pattern in value_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            val = clean_numeric(match.group(1))
            if val is not None and val > 0:
                value_redeemed = val
                break

    if shares_redeemed is not None or value_redeemed is not None:
        return RedemptionRecord(
            as_of_date=as_of,
            shares_redeemed=shares_redeemed,
            value_redeemed=value_redeemed,
            source_form_type="SC TO-I/A",
        )
    return None
