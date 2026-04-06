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
import calendar
from datetime import date
from decimal import Decimal

import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.parsers.base import (
    ParsedFiling, NavRecord, DistributionRecord, SharesIssuedRecord,
    RedemptionRecord,
)
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric, normalize_share_class, normalize_offering_type,
)

logger = logging.getLogger(__name__)


def _to_month_end(d: date, prior_if_early: bool = False) -> date:
    """Normalize a date to the last day of its month.

    Args:
        d: The date to normalize.
        prior_if_early: If True and day <= 15, map to *prior* month-end.
            Use for NAV dates where an early-month offering date (e.g., Jan 3)
            refers to the prior month's valuation (Dec 31).
            Use False for distributions where the declaration date (e.g., Feb 1)
            refers to the current month.
    """
    if prior_if_early and d.day <= 15:
        if d.month == 1:
            return date(d.year - 1, 12, 31)
        else:
            last_day = calendar.monthrange(d.year, d.month - 1)[1]
            return date(d.year, d.month - 1, last_day)
    else:
        last_day = calendar.monthrange(d.year, d.month)[1]
        return date(d.year, d.month, last_day)


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

    # Look for the NAV table — it has a very specific structure:
    # Row 0: ['NAV as of <date>'] (single-cell header with "NAV" and a date)
    # Row 1+: ['Class X Common Shares', '$xx.xx']
    table = None
    nav_table_date = None

    for t in tables:
        if not t:
            continue
        # Check if first row is a single-cell header containing "NAV" and a date
        first_row_text = " ".join(t[0]).lower() if t[0] else ""
        if len(t[0]) <= 2 and "nav" in first_row_text and re.search(r"\d{4}", first_row_text):
            # Verify data rows have share classes with dollar values
            has_class_data = any(
                _extract_share_class(row) and any("$" in cell for cell in row)
                for row in t[1:5]
            )
            if has_class_data:
                table = t
                # Extract date from header
                nav_table_date = extract_as_of_date(first_row_text)
                break

    # Fallback: look for a table where rows have "Class X" + dollar amounts
    # but NOT footnote tables (which have long text paragraphs)
    if not table:
        for t in tables:
            if not t or len(t) < 2:
                continue
            # Skip tables with very long text cells (footnotes)
            max_cell_len = max(len(cell) for row in t for cell in row)
            if max_cell_len > 200:
                continue
            header_text = " ".join(t[0]).lower()
            if "nav" in header_text or "net asset" in header_text:
                has_class_data = any(
                    _extract_share_class(row) and any("$" in cell for cell in row)
                    for row in t[1:5]
                )
                if has_class_data:
                    table = t
                    nav_table_date = extract_as_of_date(header_text)
                    break

    if not table:
        return records

    raw_date = nav_table_date or as_of or filing_date
    # NAV is always valued at month-end; early-month dates refer to prior month
    effective_date = _to_month_end(raw_date, prior_if_early=True)

    # Scan data rows for share class + NAV value
    records.extend(_scan_rows_for_nav(table, effective_date))

    return records


def _parse_distribution_table(
    tables: list[list[list[str]]],
    full_text: str,
    as_of: date | None,
    filing_date: date,
) -> list[DistributionRecord]:
    """Extract distribution per share records from tables."""
    records = []

    # Find the distribution table — must have "distribution" or "dividend" in header
    # AND contain share class rows with dollar values (not footnote tables)
    table = None
    for t in tables:
        if not t or len(t) < 2:
            continue
        # Skip tables with very long text cells (footnotes)
        max_cell_len = max(len(cell) for row in t for cell in row)
        if max_cell_len > 200:
            continue
        header_text = " ".join(" ".join(row) for row in t[:2]).lower()
        if "distribution" in header_text or "dividend" in header_text:
            has_class_data = any(
                _extract_share_class(row) and any("$" in cell for cell in row)
                for row in t[1:5]
            )
            if has_class_data:
                table = t
                break
    if not table:
        return records

    raw_date = as_of or filing_date

    # Look for distribution-specific date in the table context
    dist_date = _extract_date_near_keyword(full_text, ["distribution", "dividend"])
    if dist_date:
        raw_date = dist_date

    # Distributions are declared for a given month; normalize to month-end
    effective_date = _to_month_end(raw_date)

    # Scan data rows for class + first numeric value (gross distribution)
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

    # Look for the cumulative shares table — must contain "shares issued" as a phrase
    table = None
    for t in tables:
        header_text = " ".join(" ".join(row) for row in t[:3]).lower()
        if "shares issued" in header_text and ("consideration" in header_text or "offering" in header_text):
            table = t
            break
    if not table:
        return records

    # Shares are issued on the 1st of each month; the table reports cumulative
    # totals "as of the date of this filing", so use filing_date (not as_of,
    # which refers to the NAV valuation date from the prior month)
    effective_date = filing_date.replace(day=1)

    # Detect scale factor from table header (e.g., "dollar amounts in millions")
    consid_scale = Decimal("1")
    header_text = " ".join(" ".join(row) for row in table[:2]).lower()
    if "in millions" in header_text:
        consid_scale = Decimal("1000000")
    elif "in thousands" in header_text:
        consid_scale = Decimal("1000")
    elif "in billions" in header_text:
        consid_scale = Decimal("1000000000")

    # Scan rows for offering type headers and share class + numeric data
    current_offering_type = None
    for row in table:
        if not row:
            continue

        row_text = " ".join(row).lower().strip()

        # Skip "Total" summary rows
        if row_text.startswith("total"):
            continue

        # Check if this is an offering type header row
        stripped = re.sub(r"[:\s*]+$", "", row_text)
        if stripped in ("offering", "private offering", "private placements",
                        "private placement", "registered offering",
                        "drip", "distribution reinvestment plan"):
            current_offering_type = normalize_offering_type(stripped)
            continue

        # Check if the row itself contains an offering type keyword
        offering_in_row = _extract_offering_type(row)
        if offering_in_row:
            current_offering_type = offering_in_row

        # Check if this row has a share class
        class_match = _extract_share_class(row)
        if class_match:
            # Use current offering type, or "Primary" as default if none set
            offering = current_offering_type or "Primary"

            # Extract numeric values from the row
            numerics = []
            for cell in row:
                val = clean_numeric(cell)
                if val is not None:
                    numerics.append(val)

            shares_val = numerics[0] if len(numerics) > 0 else None
            consid_val = numerics[1] if len(numerics) > 1 else None

            # Apply scale factor to consideration
            if consid_val is not None and consid_scale != Decimal("1"):
                consid_val = consid_val * consid_scale

            if shares_val is not None or consid_val is not None:
                records.append(SharesIssuedRecord(
                    as_of_date=effective_date,
                    share_class=class_match,
                    offering_type=offering,
                    cumulative_shares=shares_val,
                    cumulative_consideration=consid_val,
                ))

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
    """Scan table rows looking for share class + dollar value patterns."""
    records = []
    for row in table:
        class_match = _extract_share_class(row)
        if not class_match:
            continue
        # Look for dollar values — must contain '$' to be a real price
        for cell in row[1:]:  # Skip first cell (class name)
            if "$" not in cell:
                continue
            val = clean_numeric(cell)
            if val is not None and Decimal("5") <= val <= Decimal("100"):
                records.append(NavRecord(
                    as_of_date=effective_date,
                    share_class=class_match,
                    nav_per_share=val,
                ))
                break
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


def has_tender_exhibit_references(html: str) -> bool:
    """Check if an 8-K filing references exhibits that may contain tender data.

    Returns True if the 8-K references Exhibit 99.x documents. These exhibits
    (shareholder letters, FAQs) often contain preliminary tender offer results.
    The exhibit parser itself filters for tender relevance, so this check is
    intentionally broad — the cost of fetching an irrelevant exhibit is low.
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True).lower()
    return bool(re.search(r"exhibit\s*99", text))


def parse_8k_exhibit_for_redemptions(
    exhibit_html: str,
    filing_date: date,
) -> list[RedemptionRecord]:
    """Parse an 8-K document or exhibit for tender/redemption data.

    Works on both primary 8-K documents and exhibits (shareholder letters, FAQs).
    These report tender results in prose, e.g.:
    - "approximately 18.8 million of its common shares ... were validly tendered"
    - "received total repurchase requests of 21.9% of shares outstanding"
    - "will fulfill its tender offer of 5%, on a pro rata basis"
    - "approximately $730 million"

    Returns RedemptionRecord(s) if tender data is found.
    """
    from src.parsers.utils import parse_date
    soup = BeautifulSoup(exhibit_html, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    # Must mention tender/repurchase to be relevant
    if not re.search(
        r"tender|repurchase\s+(?:request|program|offer)|shares?\s+(?:tendered|repurchased)",
        text, re.IGNORECASE,
    ):
        return []

    shares_tendered = None
    shares_redeemed = None
    value_redeemed = None
    pct_tendered = None

    # --- Determine the quarter-end as_of_date ---
    as_of = None
    # Strategy 1: "In the first/second/third/fourth quarter of YYYY" near
    # tender/repurchase context — most reliable for shareholder letters
    q_ref = re.search(
        r"(?:in|for|during)\s+the\s+(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter\s+of\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if q_ref:
        q_name = q_ref.group(1).lower()
        year = int(q_ref.group(2))
        q_map = {"first": 3, "1st": 3, "second": 6, "2nd": 6,
                 "third": 9, "3rd": 9, "fourth": 12, "4th": 12}
        month = q_map.get(q_name)
        if month:
            last_day = calendar.monthrange(year, month)[1]
            as_of = date(year, month, last_day)
    # Strategy 2: tender offer expiry date → snap to that quarter-end
    if not as_of:
        expiry = re.search(
            r"(?:offer|repurchase\s+program)\s+(?:that\s+)?expired?\s+(?:on\s+)?(\w+\s+\d{1,2},?\s+\d{4})",
            text, re.IGNORECASE,
        )
        if expiry:
            exp_date = parse_date(expiry.group(1))
            if exp_date:
                as_of = _snap_to_quarter_end(exp_date)
    # Strategy 3: explicit "quarter ended" or "period ended"
    if not as_of:
        qe_match = re.search(
            r"for\s+the\s+(?:quarter|period)\s+end(?:ed|ing)\s+(\w+\s+\d{1,2},?\s+\d{4})",
            text, re.IGNORECASE,
        )
        if qe_match:
            as_of = parse_date(qe_match.group(1))
    if not as_of:
        as_of = _snap_to_quarter_end(filing_date)

    # --- Shares tendered (absolute) ---
    # Pattern: "approximately X million ... shares ... were validly tendered"
    abs_patterns = [
        # "approximately 18.8 million of its common shares ... were validly tendered"
        r"(?:approximately\s+)?([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?\s+(?:of\s+its\s+)?(?:outstanding\s+|common\s+)?(?:[Ss]hares?|Shares).*?were\s+(?:validly\s+)?tendered",
        # "X shares were validly tendered"
        r"([\d,]+(?:\.\d+)?)\s+(?:of\s+its\s+)?(?:outstanding\s+|common\s+)?[Ss]hares?\s+(?:of\s+the\s+(?:Fund|Company)\s+)?were\s+(?:validly\s+)?tendered",
    ]
    for pattern in abs_patterns:
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            val = clean_numeric(m.group(1))
            suffix = (m.group(2) or "").lower() if m.lastindex >= 2 else ""
            if val and val > 0:
                if suffix == "billion":
                    val *= Decimal("1000000000")
                elif suffix == "million":
                    val *= Decimal("1000000")
                elif suffix == "thousand":
                    val *= Decimal("1000")
                shares_tendered = val
                break

    # --- Shares tendered (percentage) ---
    # "repurchase requests of/totaling X% of shares outstanding"
    # "requests to repurchase approximately X% of outstanding shares"
    pct_patterns = [
        r"(?:repurchase\s+request|tender(?:ed)?|redemption\s+request)s?\s+(?:of\s+|to\s+repurchase\s+)?(?:(?:an\s+)?estimated\s+)?(?:total(?:ing)?\s+)?(?:of\s+)?"
        r"(?:approximately\s+)?([\d.]+)%\s*\d?\s+of\s+(?:aggregate\s+)?(?:outstanding\s+)?shares",
        # "requests to repurchase approximately X% of outstanding shares"
        r"requests?\s+to\s+repurchase\s+(?:approximately\s+)?([\d.]+)%\s*\d?\s+of\s+(?:aggregate\s+)?(?:outstanding\s+)?(?:shares|Shares)",
        # "or X% of its outstanding ... shares ... were validly tendered"
        r"(?:or\s+)?(?:approximately\s+)?([\d.]+)%\s+of\s+(?:its\s+)?(?:aggregate\s+)?outstanding\s+(?:common\s+)?[Ss]hares.*?were\s+(?:validly\s+)?tendered",
    ]
    for pattern in pct_patterns:
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            try:
                pct_tendered = Decimal(m.group(1))
            except Exception:
                pass
            break

    # --- Dollar value ---
    # Look for dollar amounts near tender/repurchase/redemption context.
    # Order matters: prefer patterns tied to redemption over generic ones.
    value_patterns = [
        # "tender offer of/represents $X million"
        r"(?:tender\s+offer|repurchase\s+(?:amount|program|offer))\s+(?:represents?|of|was|is|totals?|totaling|totalling)?\s*"
        r"(?:approximately\s+)?\$\s*([\d,.]+)\s*(billion|million|thousand)?",
        # "$X million tender offer"
        r"\$\s*([\d,.]+)\s*(billion|million|thousand)?\s+(?:tender\s+offer|repurchase)",
        # "represent approximately $X million of gross outflows/redemptions"
        r"represent\s+(?:approximately\s+)?\$\s*([\d,.]+)\s*(billion|million|thousand)?\s+(?:of\s+)?(?:gross\s+)?(?:outflows?|redemptions?)",
        # "total of $X, representing"
        r"total\s+of\s+\$\s*([\d,.]+)\s*(billion|million|thousand)?\s*,?\s*representing",
        # "(approx. $X million)" or "(approx . $X million)" — note space-before-period
        r"\(?approx\s*\.?\s*\$\s*([\d,.]+)\s*(billion|million|thousand)?\s*\)?",
        # "honor/fulfill ... for 5% ... approximately $X million"
        r"(?:honor|fulfill)\s+.{0,200}?(?:approximately\s+)?\$\s*([\d,.]+)\s*(billion|million|thousand)?",
    ]
    for pattern in value_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = clean_numeric(m.group(1))
            suffix = (m.group(2) or "").lower()
            if val is not None and val > 0:
                if suffix == "billion":
                    val *= Decimal("1000000000")
                elif suffix == "million":
                    val *= Decimal("1000000")
                elif suffix == "thousand":
                    val *= Decimal("1000")
                value_redeemed = val
                break

    # If no data found, skip
    if shares_tendered is None and pct_tendered is None and value_redeemed is None:
        return []

    # Build the record
    # Detect pro rata: tender requests exceed the cap
    has_pro_rata = bool(re.search(r"pro\s+rata", text, re.IGNORECASE))
    # Also detect when tender % exceeds fulfillment % (implies pro rata)
    if not has_pro_rata and pct_tendered is not None:
        cap = re.search(
            r"(?:fulfill|honor|satisfy|offer\s+to\s+(?:repurchase|purchase))\s+.*?([\d.]+)%\s+of\s+(?:aggregate\s+)?(?:outstanding\s+)?shares",
            text, re.IGNORECASE | re.DOTALL,
        )
        if cap:
            try:
                cap_pct = Decimal(cap.group(1))
                if pct_tendered > cap_pct:
                    has_pro_rata = True
            except Exception:
                pass

    if shares_tendered is None and shares_redeemed is None and value_redeemed is not None:
        # Only dollar value found — still useful
        pass
    elif shares_tendered is not None and not has_pro_rata:
        shares_redeemed = shares_tendered

    return [RedemptionRecord(
        as_of_date=as_of,
        shares_tendered=shares_tendered,
        shares_redeemed=shares_redeemed,
        value_redeemed=value_redeemed,
        source_form_type="8-K",
    )]


def _snap_to_quarter_end(d: date) -> date:
    """Snap a date to the most recent quarter-end."""
    quarters = [(3, 31), (6, 30), (9, 30), (12, 31)]
    # Find the most recent quarter-end on or before d
    for year in [d.year, d.year - 1]:
        for month, day in reversed(quarters):
            qe = date(year, month, day)
            if qe <= d:
                return qe
    return d
