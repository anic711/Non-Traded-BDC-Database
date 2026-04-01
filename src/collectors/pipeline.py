"""Collection pipeline: orchestrates fetching, parsing, and storing SEC filing data."""

import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import FUNDS, DATA_START_DATE
from src.database import async_session_factory
from src.edgar.client import EdgarClient
from src.edgar.filing_index import extract_filings, FilingInfo
from src.models import (
    Fund, Filing, NavPerShare, Distribution, SharesIssued,
    Redemption, TotalNav, UpdateLog,
)
from src.parsers.base import ParsedFiling
from src.parsers.filing_8k import parse_8k
from src.parsers.filing_10q10k import parse_10q10k
from src.parsers.filing_sctoi import parse_sctoi

logger = logging.getLogger(__name__)


async def run_update(trigger_type: str = "manual") -> int:
    """Run a full update cycle for all active funds.

    Args:
        trigger_type: 'scheduled' or 'manual'

    Returns:
        The update_log ID for tracking
    """
    async with async_session_factory() as session:
        # Create update log entry
        log_entry = UpdateLog(
            started_at=datetime.now(timezone.utc),
            trigger_type=trigger_type,
            status="running",
        )
        session.add(log_entry)
        await session.commit()
        log_id = log_entry.id

    client = EdgarClient()
    total_processed = 0
    errors = []

    try:
        # Ensure funds are seeded
        await _ensure_funds_seeded()

        async with async_session_factory() as session:
            result = await session.execute(
                select(Fund).where(Fund.active.is_(True))
            )
            funds = result.scalars().all()

        date_from = date.fromisoformat(DATA_START_DATE)

        for fund in funds:
            try:
                processed = await _process_fund(client, fund, date_from)
                total_processed += processed
                logger.info(f"Processed {processed} new filings for {fund.ticker}")
            except Exception as e:
                error_msg = f"{fund.ticker}: {type(e).__name__}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        # Update log entry
        async with async_session_factory() as session:
            log = await session.get(UpdateLog, log_id)
            log.completed_at = datetime.now(timezone.utc)
            log.status = "completed" if not errors else "completed_with_errors"
            log.filings_processed = total_processed
            log.errors = "\n".join(errors) if errors else None
            await session.commit()

    except Exception as e:
        logger.error(f"Update pipeline failed: {e}")
        async with async_session_factory() as session:
            log = await session.get(UpdateLog, log_id)
            log.completed_at = datetime.now(timezone.utc)
            log.status = "failed"
            log.errors = str(e)
            await session.commit()
    finally:
        await client.close()

    return log_id


async def _ensure_funds_seeded():
    """Ensure all configured funds exist in the database."""
    async with async_session_factory() as session:
        for fund_data in FUNDS:
            result = await session.execute(
                select(Fund).where(Fund.ticker == fund_data["ticker"])
            )
            if not result.scalar_one_or_none():
                session.add(Fund(
                    ticker=fund_data["ticker"],
                    name=fund_data["name"],
                    cik=fund_data["cik"],
                ))
        await session.commit()


async def _process_fund(
    client: EdgarClient,
    fund: Fund,
    date_from: date,
) -> int:
    """Process all new filings for a single fund.

    Returns the number of new filings processed.
    """
    # Fetch filing index from EDGAR
    submissions = await client.get_submissions(fund.cik)
    filings = extract_filings(submissions, date_from=date_from)

    if not filings:
        logger.info(f"No relevant filings found for {fund.ticker}")
        return 0

    processed = 0
    for filing_info in filings:
        # Check if we've already processed this filing
        async with async_session_factory() as session:
            result = await session.execute(
                select(Filing).where(Filing.accession_number == filing_info.accession_number)
            )
            if result.scalar_one_or_none():
                continue  # Already processed

        try:
            await _process_filing(client, fund, filing_info)
            processed += 1
        except Exception as e:
            logger.error(
                f"Error processing {filing_info.form_type} "
                f"({filing_info.accession_number}) for {fund.ticker}: {e}"
            )
            # Store the failed filing record for debugging
            await _store_failed_filing(fund, filing_info, str(e))

    return processed


async def _process_filing(
    client: EdgarClient,
    fund: Fund,
    filing_info: FilingInfo,
):
    """Fetch, parse, and store a single filing."""
    # Fetch the document HTML
    html = await client.get_filing_document(
        cik=fund.cik,
        accession_number=filing_info.accession_number,
        document=filing_info.primary_document,
    )

    # Store the filing record with raw HTML
    async with async_session_factory() as session:
        filing = Filing(
            fund_id=fund.id,
            accession_number=filing_info.accession_number,
            form_type=filing_info.form_type,
            filing_date=filing_info.filing_date,
            primary_document_url=(
                f"https://www.sec.gov/Archives/edgar/data/{fund.cik}/"
                f"{filing_info.accession_number.replace('-', '')}/"
                f"{filing_info.primary_document}"
            ),
            raw_html=html,
            parse_status="pending",
        )
        session.add(filing)
        await session.commit()
        filing_id = filing.id

    # Parse based on form type
    parsed = _parse_filing(html, filing_info)

    # Store parsed data
    if parsed and parsed.has_data:
        await _store_parsed_data(fund.id, filing_id, parsed)
        async with async_session_factory() as session:
            filing = await session.get(Filing, filing_id)
            filing.parse_status = "success"
            filing.parsed_at = datetime.now(timezone.utc)
            await session.commit()
    else:
        async with async_session_factory() as session:
            filing = await session.get(Filing, filing_id)
            filing.parse_status = "skipped"
            filing.parse_error = "No relevant data extracted"
            filing.parsed_at = datetime.now(timezone.utc)
            await session.commit()


def _parse_filing(html: str, filing_info: FilingInfo) -> ParsedFiling | None:
    """Route to the appropriate parser based on form type."""
    try:
        if filing_info.form_type == "8-K":
            return parse_8k(html, filing_info.filing_date)
        elif filing_info.form_type in ("10-Q", "10-K"):
            return parse_10q10k(html, filing_info.filing_date, filing_info.form_type)
        elif filing_info.form_type in ("SC TO-I", "SC TO-I/A"):
            return parse_sctoi(html, filing_info.filing_date)
        else:
            logger.warning(f"Unknown form type: {filing_info.form_type}")
            return None
    except Exception as e:
        logger.error(f"Parser error for {filing_info.form_type}: {e}")
        return None


async def _store_parsed_data(fund_id: int, filing_id: int, parsed: ParsedFiling):
    """Store parsed records in the database using upsert (INSERT ON CONFLICT UPDATE)."""
    async with async_session_factory() as session:
        # NAV per share records
        for rec in parsed.nav_records:
            stmt = pg_insert(NavPerShare).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                nav_per_share=rec.nav_per_share,
            ).on_conflict_do_update(
                constraint="uq_nav_per_share",
                set_=dict(
                    filing_id=filing_id,
                    nav_per_share=rec.nav_per_share,
                ),
            )
            await session.execute(stmt)

        # Distribution records
        for rec in parsed.distribution_records:
            stmt = pg_insert(Distribution).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                distribution_per_share=rec.distribution_per_share,
            ).on_conflict_do_update(
                constraint="uq_distributions",
                set_=dict(
                    filing_id=filing_id,
                    distribution_per_share=rec.distribution_per_share,
                ),
            )
            await session.execute(stmt)

        # Shares issued records
        for rec in parsed.shares_issued_records:
            stmt = pg_insert(SharesIssued).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                offering_type=rec.offering_type,
                cumulative_shares=rec.cumulative_shares,
                cumulative_consideration=rec.cumulative_consideration,
            ).on_conflict_do_update(
                constraint="uq_shares_issued",
                set_=dict(
                    filing_id=filing_id,
                    cumulative_shares=rec.cumulative_shares,
                    cumulative_consideration=rec.cumulative_consideration,
                ),
            )
            await session.execute(stmt)

        # Redemption records
        for rec in parsed.redemption_records:
            stmt = pg_insert(Redemption).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                shares_redeemed=rec.shares_redeemed,
                value_redeemed=rec.value_redeemed,
                source_form_type=rec.source_form_type,
            ).on_conflict_do_update(
                constraint="uq_redemptions",
                set_=dict(
                    filing_id=filing_id,
                    shares_redeemed=rec.shares_redeemed,
                    value_redeemed=rec.value_redeemed,
                    source_form_type=rec.source_form_type,
                ),
            )
            await session.execute(stmt)

        # Total NAV records
        for rec in parsed.total_nav_records:
            stmt = pg_insert(TotalNav).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                total_nav=rec.total_nav,
            ).on_conflict_do_update(
                constraint="uq_total_nav",
                set_=dict(
                    filing_id=filing_id,
                    total_nav=rec.total_nav,
                ),
            )
            await session.execute(stmt)

        await session.commit()


async def _store_failed_filing(fund: Fund, filing_info: FilingInfo, error: str):
    """Store a filing record with failed status for debugging."""
    async with async_session_factory() as session:
        # Check if already stored
        result = await session.execute(
            select(Filing).where(Filing.accession_number == filing_info.accession_number)
        )
        if result.scalar_one_or_none():
            return

        filing = Filing(
            fund_id=fund.id,
            accession_number=filing_info.accession_number,
            form_type=filing_info.form_type,
            filing_date=filing_info.filing_date,
            parse_status="failed",
            parse_error=error,
        )
        session.add(filing)
        await session.commit()
