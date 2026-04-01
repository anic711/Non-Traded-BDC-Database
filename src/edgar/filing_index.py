"""Extract and filter filing lists from EDGAR submission data."""

import logging
from datetime import date, datetime
from dataclasses import dataclass

logger = logging.getLogger(__name__)

RELEVANT_FORM_TYPES = {"8-K", "10-Q", "10-K", "SC TO-I", "SC TO-I/A"}


@dataclass
class FilingInfo:
    """Metadata for a single SEC filing."""
    accession_number: str
    form_type: str
    filing_date: date
    primary_document: str


def extract_filings(
    submissions_json: dict,
    form_types: set[str] | None = None,
    date_from: date | None = None,
) -> list[FilingInfo]:
    """Extract relevant filings from EDGAR submissions JSON.

    Args:
        submissions_json: Raw JSON from data.sec.gov/submissions/CIK*.json
        form_types: Set of form types to include (default: all relevant types)
        date_from: Only include filings on or after this date

    Returns:
        List of FilingInfo objects sorted by filing_date descending
    """
    if form_types is None:
        form_types = RELEVANT_FORM_TYPES

    recent = submissions_json.get("filings", {}).get("recent", {})
    if not recent:
        logger.warning("No recent filings found in submissions data")
        return []

    accession_numbers = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i in range(len(accession_numbers)):
        form = forms[i] if i < len(forms) else ""
        if form not in form_types:
            continue

        filing_dt = datetime.strptime(filing_dates[i], "%Y-%m-%d").date()
        if date_from and filing_dt < date_from:
            continue

        filings.append(FilingInfo(
            accession_number=accession_numbers[i],
            form_type=form,
            filing_date=filing_dt,
            primary_document=primary_docs[i] if i < len(primary_docs) else "",
        ))

    # Also check for older filings in supplementary files
    older_files = submissions_json.get("filings", {}).get("files", [])
    # These would need separate fetches - handled by the pipeline if needed

    filings.sort(key=lambda f: f.filing_date, reverse=True)
    logger.info(f"Found {len(filings)} relevant filings")
    return filings
