"""Parser for SC TO-I and SC TO-I/A filings: share redemption data.

SC TO-I/A (amended issuer tender offer statement) filings report:
- Shares accepted for purchase / redeemed
- Aggregate consideration paid (value of shares redeemed)
- Offer dates, record dates, payment dates
"""

import re
import calendar
import logging
from datetime import date
from decimal import Decimal

import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.parsers.base import ParsedFiling, RedemptionRecord
from src.parsers.utils import (
    extract_tables, find_table_with_keywords, extract_as_of_date,
    clean_numeric, parse_date,
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

    # Only parse final amendments that report actual tender offer results.
    # Interim amendments report offer caps but no actual shares tendered.
    # Final amendments have the "final amendment" checkbox checked (☒).
    is_final = bool(re.search(r"final\s+amendment\s+reporting\s+the\s+results.*?☒", text, re.IGNORECASE))
    # Also accept filings that explicitly report completed results (past tense).
    # Require "were" before the verb to ensure past tense, avoiding future/conditional
    # language like "Shares that are accepted for purchase will be sent a letter..."
    has_results = bool(re.search(
        r"(?:Shares?\s+were\s+(?:validly\s+)?(?:accepted\s+for\s+purchase|purchased|repurchased))"
        r"|(?:(?:Fund|Company)\s+purchased\s+all\s+(?:validly\s+)?tendered)"
        r"|(?:Shares?\s+were\s+(?:validly\s+)?tendered\s+and\s+not\s+withdrawn\s+prior\s+to\s+the\s+expiration)"
        r"|(?:fulfilling\s+(?:all\s+)?(?:repurchase|tender|redemption)\s+requests)"
        r"|(?:meet\s+100%\s+of\s+(?:repurchase|tender|redemption)?\s*requests)"
        r"|(?:upsize\s+the\s+offer)",
        text, re.IGNORECASE,
    ))
    if not is_final and not has_results:
        logger.info("Skipping non-final SC TO-I/A amendment (no tender offer results)")
        return result

    tables = extract_tables(soup)

    # Determine as_of_date: snap to quarter-end of the quarter the offer commenced.
    # Use the earlier of "Date Tender Offer First Published" (commencement) and
    # offer expiry date — the commencement date always precedes expiry, so the
    # min handles cases where the "published" date on an amendment is actually
    # the amendment filing date (which can be in the next quarter).
    as_of = None

    def _snap_quarter_end(d):
        qm = ((d.month - 1) // 3 + 1) * 3
        return date(d.year, qm, calendar.monthrange(d.year, qm)[1])

    candidate_dates = []

    # Try offer commencement date
    pub_match = re.search(
        r"(\w+\s*\??\s*\d{1,2},?\s*\??\s*\d{4})\s*\(Date\s+Tender\s+Offer\s+First\s+Published",
        text, re.DOTALL,
    )
    if pub_match:
        raw = re.sub(r"[?\s]+", " ", pub_match.group(1)).strip()
        pub_date = parse_date(raw)
        if pub_date:
            candidate_dates.append(pub_date)

    # Try offer expiration date
    expiry_match = re.search(
        r"[Oo]ffer\s+expir\w+.*?(?:on\s+)?(\w+\s*\??\s*\d{1,2},?\s*\??\s*\d{4})",
        text, re.DOTALL,
    )
    if expiry_match:
        raw = re.sub(r"[?\s]+", " ", expiry_match.group(1)).strip()
        expiry_date = parse_date(raw)
        if expiry_date:
            candidate_dates.append(expiry_date)

    if candidate_dates:
        as_of = _snap_quarter_end(min(candidate_dates))

    if not as_of:
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
    shares_tendered = None
    value_redeemed = None

    # --- "Validly tendered and not withdrawn" ---
    # In SC TO-I/A filings, this phrase reports the shares that were ACCEPTED
    # for purchase (after any proration), not necessarily total requests.
    # We call this 'validly_tendered' and use it for shares_redeemed.
    validly_tendered = None
    tendered_patterns = [
        r"([\d,]+(?:\.\d+)?)\s+[Ss]hares?\s+(?:of\s+the\s+(?:Fund|Company)\s+)?were\s+(?:validly\s+)?tendered\s+and\s+not\s+withdrawn",
        r"([\d,]+(?:\.\d+)?)\s+shares?\s+were\s+(?:validly\s+)?tendered",
    ]
    for pattern in tendered_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            val = clean_numeric(match.group(1))
            if val is not None and val > 0:
                validly_tendered = val
                break

    # Per-class tendered: "X Class S Shares, Y Class D Shares ... were validly tendered"
    if validly_tendered is None:
        tendered_ctx = re.search(
            r"([\d,]+\s+Class\s+[A-Z][\w\s,]+?Shares?(?:\s*,?\s*(?:and\s+)?[\d,]+\s+Class\s+[A-Z][\w\s]*?Shares?)*)\s+were\s+(?:validly\s+)?tendered",
            text, re.IGNORECASE,
        )
        if tendered_ctx:
            per_class = re.findall(r"([\d,]+(?:\.\d+)?)\s+Class\s+[A-Z]", tendered_ctx.group(1))
            total = Decimal("0")
            for s in per_class:
                val = clean_numeric(s)
                if val is not None and val > 0:
                    total += val
            if total > 0:
                validly_tendered = total

    # --- Shares redeemed/accepted ---
    # Check if filing explicitly states a different accepted amount (pro rata)
    accepted_match = re.search(
        r"accepted\s+for\s+purchase\s+([\d,]+(?:\.\d+)?)\s+[Ss]hares",
        text, re.IGNORECASE,
    )
    if accepted_match:
        val = clean_numeric(accepted_match.group(1))
        if val is not None and val > 0:
            shares_redeemed = val
            # When accepted < validly_tendered, the validly_tendered IS the
            # actual total requests (pro rata scenario, e.g. ASIF)
            if validly_tendered is not None and validly_tendered > val:
                shares_tendered = validly_tendered

    # If "accepted for purchase 100%", redeemed = validly_tendered
    if shares_redeemed is None and validly_tendered is not None and re.search(
        r"accepted\s+for\s+(?:purchase|payment)\s+100%", text, re.IGNORECASE,
    ):
        shares_redeemed = validly_tendered
    # "purchased all validly tendered" means 100%
    elif shares_redeemed is None and validly_tendered is not None and re.search(
        r"purchased\s+all\s+(?:validly\s+)?tendered", text, re.IGNORECASE,
    ):
        shares_redeemed = validly_tendered

    # Other patterns for shares accepted/redeemed
    if shares_redeemed is None:
        shares_patterns = [
            r"([\d,]+(?:\.\d+)?)\s+[Ss]hares?\s+(?:of\s+the\s+(?:Fund|Company)\s+)?were\s+(?:validly\s+)?(?:accepted|repurchased|purchased)",
            r"(?:accepted|repurchased|purchased)\s+([\d,]+(?:\.\d+)?)\s+[Ss]hares",
            r"repurchased\s+(?:a\s+total\s+of\s+)?([\d,]+(?:\.\d+)?)\s+[Ss]hares",
        ]
        for pattern in shares_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = clean_numeric(match.group(1))
                if val is not None and val > 0:
                    shares_redeemed = val
                    break

    # If no single total found, sum per-class shares for accepted/repurchased
    if shares_redeemed is None:
        per_class = re.findall(
            r"([\d,]+(?:\.\d+)?)\s+Class\s+[A-Z]\s+(?:Common\s+)?Shares?",
            text, re.IGNORECASE,
        )
        if per_class and re.search(
            r"Class\s+[A-Z]\s+(?:Common\s+)?Shares?\s+were\s+(?:validly\s+)?(?:accepted|repurchased|purchased)",
            text, re.IGNORECASE,
        ):
            total = Decimal("0")
            for s in per_class:
                val = clean_numeric(s)
                if val is not None and val > 0:
                    total += val
            if total > 0:
                shares_redeemed = total

    # Default: if validly_tendered found but shares_redeemed still not, use it
    if shares_redeemed is None and validly_tendered is not None:
        has_partial = bool(re.search(
            r"pro\s+rata|representing\s+\d+(?:\.\d+)?%\s+of\s+the\s+(?:Shares|shares)",
            text, re.IGNORECASE,
        ))
        if not has_partial:
            shares_redeemed = validly_tendered

    # --- Value redeemed ---
    value_patterns = [
        r"total\s+of\s+\$\s*([\d,]+(?:\.\d+)?)\s*,?\s*representing\s+(?:\d+%\s+of\s+)?the\s+net\s+asset\s+value",
        r"net\s+asset\s+value\s+of\s+[Ss]hares?\s+tendered.{0,120}?(?:amount\s+of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"(?:aggregate|total)\s+(?:net\s+asset\s+)?value\s+(?:of\s+)?shares?\s+tendered[^$]*?(?:amount\s+of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"(?:aggregate|total)\s+consideration\s+(?:paid\s+)?(?:was\s+|of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"(?:aggregate|total)\s+(?:purchase\s+price|cost)\s+(?:of\s+|was\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"(?:aggregate|total)\s+(?:purchase\s+price|cost)\s+.{0,100}?(?:was\s+)?(?:approximately\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"repurchased\s+with\s+\$\s*([\d,]+(?:\.\d+)?)",
        r"\$\s*([\d,]+(?:\.\d+)?)\s+(?:in\s+)?(?:aggregate|total)\s+(?:consideration|purchase)",
        r"(?:aggregate|total)\s+(?:repurchase|redemption)\s+(?:amount|price)\s+(?:of\s+|was\s+)?\$\s*([\d,]+(?:\.\d+)?)",
        r"consideration\s+(?:paid\s+)?(?:was\s+|of\s+)?\$\s*([\d,]+(?:\.\d+)?)",
    ]
    for pattern in value_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            val = clean_numeric(match.group(1))
            if val is not None and val > 0:
                value_redeemed = val
                break

    # --- Offer cap with upsize and 100% fulfillment ---
    # When filing says "fulfilling all requests" / "100% of requests" with
    # "purchase up to X shares" and an upsize, compute actual from percentages.
    # The "up to X shares" is the ORIGINAL offer (e.g. 5%), not the actual amount.
    if shares_redeemed is None and validly_tendered is None:
        full_fulfillment = bool(re.search(
            r"fulfilling\s+(?:all\s+)?(?:repurchase|tender)?\s*requests"
            r"|meet\s+100%\s+of\s+(?:repurchase|tender)?\s*requests",
            text, re.IGNORECASE,
        ))
        if full_fulfillment:
            cap_match = re.search(
                r"purchase\s+up\s+to\s+([\d,]+(?:\.\d+)?)\s+(?:of\s+its\s+)?(?:outstanding\s+)?shares",
                text, re.IGNORECASE,
            )
            if cap_match:
                original_cap = clean_numeric(cap_match.group(1))
                if original_cap is not None and original_cap > 0:
                    original_pct = Decimal("5")  # default 5%
                    total_pct = original_pct

                    upsize = re.search(
                        r"upsize\s+the\s+offer\s+to\s+([\d.]+)%",
                        text, re.IGNORECASE,
                    )
                    if upsize:
                        total_pct = Decimal(upsize.group(1))

                    redeemed_pct = total_pct

                    offset_pct = Decimal("0")
                    offset = re.search(
                        r"(?:approximately|representing)\s+([\d.]+)%\s+of\s+\w+(?:['\u2019]s)?\s+shares\s+outstanding",
                        text, re.IGNORECASE,
                    )
                    if offset:
                        offset_pct = Decimal(offset.group(1))

                    shares_outstanding = original_cap / (original_pct / Decimal("100"))
                    tendered_pct = redeemed_pct + offset_pct
                    shares_tendered = ((tendered_pct / Decimal("100")) * shares_outstanding).quantize(Decimal("1"))
                    shares_redeemed = ((redeemed_pct / Decimal("100")) * shares_outstanding).quantize(Decimal("1"))

    # --- Total tender requests (actual investor demand) ---
    # This captures the total repurchase requests submitted, which may be much
    # higher than what was accepted. Look for explicit request % of shares O/S.
    if shares_tendered is None:
        request_pct_match = re.search(
            r"(?:requests?\s+to\s+(?:repurchase|redeem|tender)\s+(?:approximately\s+)?([\d.]+)%)"
            r"|(?:(?:repurchase|redemption|tender)\s+requests?\s+(?:of|for|totaling)\s+(?:approximately\s+)?([\d.]+)%)"
            r"|(?:(?:repurchase|redemption|tender)\s+requests?\s+(?:exceeded|were)\s+(?:approximately\s+)?([\d.]+)%)",
            text, re.IGNORECASE,
        )
        if request_pct_match:
            pct_str = next(g for g in request_pct_match.groups() if g is not None)
            request_pct = Decimal(pct_str) / Decimal("100")
            # Derive shares outstanding from the offer cap if available
            cap_match = re.search(
                r"purchase\s+up\s+to\s+([\d,]+(?:\.\d+)?)\s+(?:of\s+its\s+)?(?:outstanding\s+)?shares",
                text, re.IGNORECASE,
            )
            if cap_match:
                original_cap = clean_numeric(cap_match.group(1))
                if original_cap is not None and original_cap > 0:
                    shares_os = original_cap / Decimal("0.05")  # 5% offer
                    shares_tendered = (request_pct * shares_os).quantize(Decimal("1"))

        # Fulfillment rate: "approximately X% of their requested capital"
        if shares_tendered is None:
            fulfill_match = re.search(
                r"(?:approximately|roughly|about|estimated?)\s+([\d.]+)%\s+of\s+their\s+requested\s+capital",
                text, re.IGNORECASE,
            )
            if fulfill_match and (shares_redeemed is not None or value_redeemed is not None):
                fulfill_pct = Decimal(fulfill_match.group(1)) / Decimal("100")
                if fulfill_pct > 0:
                    if shares_redeemed is not None:
                        shares_tendered = (shares_redeemed / fulfill_pct).quantize(Decimal("1"))
                    elif value_redeemed is not None:
                        # We'll store this and let the service layer infer shares
                        pass

    # For 100% acceptance with no exceeded/upsize language, tendered = redeemed
    if shares_tendered is None and shares_redeemed is not None:
        exceeded = bool(re.search(r"exceed", text, re.IGNORECASE))
        if not exceeded:
            shares_tendered = shares_redeemed

    if shares_redeemed is not None or value_redeemed is not None or shares_tendered is not None:
        return RedemptionRecord(
            as_of_date=as_of,
            shares_redeemed=shares_redeemed,
            value_redeemed=value_redeemed,
            source_form_type="SC TO-I/A",
            shares_tendered=shares_tendered,
        )
    return None
