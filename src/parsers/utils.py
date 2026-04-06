"""Shared parsing utilities for SEC filing HTML extraction."""

import re
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# Common date patterns found in SEC filings
DATE_PATTERNS = [
    # "As of March 31, 2024"
    r"[Aa]s\s+of\s+(\w+\s+\d{1,2},?\s+\d{4})",
    # "March 31, 2024"
    r"(\w+\s+\d{1,2},?\s+\d{4})",
    # "03/31/2024" or "3/31/2024"
    r"(\d{1,2}/\d{1,2}/\d{4})",
    # "2024-03-31"
    r"(\d{4}-\d{2}-\d{2})",
]

DATE_FORMATS = [
    "%B %d, %Y",      # March 31, 2024
    "%B %d %Y",       # March 31 2024
    "%b %d, %Y",      # Mar 31, 2024
    "%m/%d/%Y",        # 03/31/2024
    "%Y-%m-%d",        # 2024-03-31
]


def parse_date(text: str) -> date | None:
    """Try to parse a date from various formats found in SEC filings."""
    text = text.strip().replace(",", ", ").replace("  ", " ")
    # Remove commas that aren't followed by a space (cleanup)
    text = re.sub(r",(\S)", r", \1", text)

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text.strip().rstrip(","), fmt).date()
        except ValueError:
            continue
    return None


def extract_as_of_date(html_text: str) -> date | None:
    """Extract the 'as of' date from filing HTML text.

    Looks for patterns like 'As of March 31, 2024' or similar.
    """
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, html_text)
        if match:
            result = parse_date(match.group(1))
            if result:
                return result
    return None


def clean_numeric(text: str) -> Decimal | None:
    """Parse a numeric value from text, handling $, commas, parentheses, %, etc.

    Returns None if the text doesn't contain a parseable number.
    Handles suffixes like 'billion', 'million', 'thousand'.
    """
    if not text:
        return None

    text = text.strip()

    # Handle common "zero" representations
    if text in ("N/A", "n/a", "N/A ", ""):
        return None
    # Em dash / en dash / hyphen alone = zero (SEC EDGAR convention for $0)
    if text in ("-", "—", "–"):
        return Decimal("0")

    # Check for multiplier suffixes
    multiplier = Decimal("1")
    text_lower = text.lower()
    if "billion" in text_lower:
        multiplier = Decimal("1000000000")
        text = re.sub(r"\s*billion\s*", "", text, flags=re.IGNORECASE)
    elif "million" in text_lower:
        multiplier = Decimal("1000000")
        text = re.sub(r"\s*million\s*", "", text, flags=re.IGNORECASE)
    elif "thousand" in text_lower:
        multiplier = Decimal("1000")
        text = re.sub(r"\s*thousand\s*", "", text, flags=re.IGNORECASE)

    # Check for parentheses (negative numbers)
    is_negative = bool(re.search(r"\(.*\)", text))

    # After removing suffixes, check if remaining is just a dash/em dash (= zero)
    remaining = re.sub(r"[$,\s()]", "", text).strip()
    if remaining in ("-", "—", "–", ""):
        return Decimal("0") * multiplier  # e.g. "$— billion" = 0

    # Strip non-numeric characters except decimal point and minus
    cleaned = re.sub(r"[^\d.\-]", "", text)

    if not cleaned or cleaned in (".", "-"):
        return None

    try:
        value = Decimal(cleaned) * multiplier
        if is_negative:
            value = -value
        return value
    except InvalidOperation:
        return None


def _merge_currency_cells(cells: list[str]) -> list[str]:
    """Merge adjacent cells that form a single value.

    SEC EDGAR HTML often splits '$' and the number into separate <td> cells,
    e.g. ['$', '24.38', ''] or ['$', '17.3', 'billion'].
    This merges them back together.
    """
    merged = []
    i = 0
    while i < len(cells):
        cell = cells[i].strip()
        # If this cell is just '$' or '($', merge with the next non-empty cell
        if cell in ("$", "($") and i + 1 < len(cells):
            next_cell = cells[i + 1].strip()
            if next_cell:
                # Check if there's a 'billion'/'million' suffix after
                combined = cell + next_cell
                if i + 2 < len(cells) and cells[i + 2].strip().lower() in ("billion", "million", "thousand"):
                    combined += " " + cells[i + 2].strip()
                    i += 3
                else:
                    i += 2
                # If original was '($', check for closing paren
                if cell == "($" and not combined.endswith(")"):
                    combined += ")"
                merged.append(combined)
                continue
            else:
                i += 1
                continue
        # Skip empty cells
        if not cell:
            i += 1
            continue
        # Merge a numeric cell followed by a multiplier suffix (e.g. ['1.3', 'billion'])
        if re.match(r"^[\d,.]+$", cell) and i + 1 < len(cells) and cells[i + 1].strip().lower() in ("billion", "million", "thousand"):
            combined = cell + " " + cells[i + 1].strip()
            merged.append(combined)
            i += 2
            continue
        merged.append(cell)
        i += 1
    return merged


def extract_tables(soup: BeautifulSoup) -> list[list[list[str]]]:
    """Extract all HTML tables as lists of rows of cell text.

    Returns a list of tables, where each table is a list of rows,
    and each row is a list of cell text strings.
    Adjacent cells that form a single value (e.g. '$' + '24.38') are merged.
    """
    tables = []
    for table_tag in soup.find_all("table"):
        rows = []
        for tr in table_tag.find_all("tr"):
            cells = []
            for td in tr.find_all(["td", "th"]):
                text = td.get_text(separator=" ", strip=True)
                # Normalize whitespace
                text = re.sub(r"\s+", " ", text)
                cells.append(text)
            if cells and any(c.strip() for c in cells):
                merged = _merge_currency_cells(cells)
                if merged:
                    rows.append(merged)
        if rows:
            tables.append(rows)
    return tables


def find_table_with_keywords(
    tables: list[list[list[str]]],
    keywords: list[str],
    min_matches: int = 1,
) -> list[list[str]] | None:
    """Find the first table containing the specified keywords in its header row(s).

    Args:
        tables: List of extracted tables
        keywords: Keywords to search for (case-insensitive)
        min_matches: Minimum number of keywords that must match

    Returns:
        The matching table (list of rows) or None
    """
    keywords_lower = [k.lower() for k in keywords]
    for table in tables:
        # Check first 3 rows for keywords (headers can span multiple rows)
        header_text = " ".join(
            " ".join(row) for row in table[:3]
        ).lower()
        matches = sum(1 for kw in keywords_lower if kw in header_text)
        if matches >= min_matches:
            return table
    return None


def normalize_share_class(text: str) -> str:
    """Normalize share class names to a consistent format.

    Examples:
        'Class I' -> 'Class I'
        'Class I Shares' -> 'Class I'
        'CLASS I' -> 'Class I'
        'Class S' -> 'Class S'
    """
    text = text.strip()
    match = re.search(r"[Cc]lass\s+([A-Z])", text, re.IGNORECASE)
    if match:
        return f"Class {match.group(1).upper()}"
    return text


def normalize_offering_type(text: str) -> str:
    """Normalize offering type names.

    Examples:
        'Distribution Reinvestment Plan' -> 'DRIP'
        'DRIP' -> 'DRIP'
        'Primary Offering' -> 'Primary'
    """
    text_lower = text.strip().lower()
    if "reinvestment" in text_lower or "drip" in text_lower:
        return "DRIP"
    if "private" in text_lower:
        return "Private Placement"
    if "registered" in text_lower:
        return "Primary"
    if "primary" in text_lower or text_lower in ("offering", "offering:"):
        return "Primary"
    return text.strip()


def find_item_section(soup: BeautifulSoup, item_number: str) -> Tag | None:
    """Find the HTML section for a specific 8-K Item number (e.g., '7.01', '8.01').

    Returns the parent element containing that item's content.
    """
    # Look for text like "Item 7.01" or "ITEM 7.01"
    pattern = re.compile(rf"Item\s+{re.escape(item_number)}", re.IGNORECASE)

    for element in soup.find_all(string=pattern):
        # Return the parent container that likely holds the full section
        parent = element.find_parent(["div", "body", "td", "p"])
        if parent:
            return parent
    return None
