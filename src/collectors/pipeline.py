"""Collection pipeline: orchestrates fetching, parsing, and storing SEC filing data."""

import logging
import re
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import FUNDS, DATA_START_DATE
from src.database import async_session_factory
from src.edgar.client import EdgarClient
from src.edgar.filing_index import extract_filings, FilingInfo
from src.models import (
    Fund, Filing, NavPerShare, Distribution, SharesIssued,
    Redemption, TotalNav, SharesOutstanding, UpdateLog,
)
from src.parsers.base import ParsedFiling
from src.parsers.filing_8k import (
    parse_8k, has_tender_exhibit_references, parse_8k_exhibit_for_redemptions,
)
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
                error_msg = f"{fund.ticker}: {type(e).__name__}: {str(e)[:200]}"
                logger.error(error_msg)
                errors.append(error_msg)

        # Backfill redemptions from 8-K exhibits (for filings already in DB)
        await backfill_8k_exhibit_redemptions()

        # Backfill any missing redemption values using NAV × shares
        await backfill_redemption_values()

        # Update log entry
        async with async_session_factory() as session:
            log = await session.get(UpdateLog, log_id)
            log.completed_at = datetime.now(timezone.utc)
            log.status = "completed" if not errors else "partial"
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

    # For 8-K filings, check for tender/redemption data in:
    # 1. The primary document itself (some BDCs embed tender results here)
    # 2. Exhibits (shareholder letters, FAQs)
    if filing_info.form_type == "8-K":
        primary_redemptions = parse_8k_exhibit_for_redemptions(html, filing_info.filing_date)
        if primary_redemptions:
            if parsed is None:
                parsed = ParsedFiling()
            parsed.redemption_records.extend(primary_redemptions)
            logger.info(
                f"Found {len(primary_redemptions)} redemption records "
                f"in 8-K primary doc for {fund.ticker}"
            )

        if has_tender_exhibit_references(html):
            exhibit_redemptions = await _fetch_and_parse_8k_exhibits(
                client, fund.cik, filing_info,
            )
            if exhibit_redemptions:
                if parsed is None:
                    parsed = ParsedFiling()
                parsed.redemption_records.extend(exhibit_redemptions)
                logger.info(
                    f"Found {len(exhibit_redemptions)} redemption records "
                    f"in 8-K exhibits for {fund.ticker}"
                )

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


async def _fetch_and_parse_8k_exhibits(
    client: EdgarClient,
    cik: str,
    filing_info: FilingInfo,
) -> list:
    """Fetch and parse 8-K exhibit documents for tender/redemption data.

    Looks for ex-99.x exhibits (shareholder letters, FAQs) that contain
    preliminary tender offer results.
    """
    try:
        index_data = await client.get_filing_index(cik, filing_info.accession_number)
    except Exception as e:
        logger.warning(f"Failed to fetch filing index for {filing_info.accession_number}: {e}")
        return []

    # Find exhibit documents — include both standard (ex99*, ex-99*) and
    # non-standard names. Exclude primary doc, XBRL viewer pages (R*.htm),
    # and index pages.
    primary_doc = filing_info.primary_document.lower()
    items = index_data.get("directory", {}).get("item", [])
    exhibit_files = []
    for item in items:
        name = item.get("name", "").lower()
        if not name.endswith((".htm", ".html")):
            continue
        # Skip primary document, XBRL viewer pages, and index pages
        if name == primary_doc:
            continue
        if re.match(r"^r\d+\.htm", name):
            continue
        if "index" in name:
            continue
        # Include ex-99 exhibits and any other non-standard HTML exhibits
        if re.search(r"ex-?99", name) or not name.startswith(("r", "0")):
            exhibit_files.append(item["name"])

    if not exhibit_files:
        return []

    # Deduplicate by as_of_date — multiple exhibits may report the same data
    seen_dates = {}
    for exhibit_name in exhibit_files:
        try:
            exhibit_html = await client.get_filing_document(
                cik=cik,
                accession_number=filing_info.accession_number,
                document=exhibit_name,
            )
            records = parse_8k_exhibit_for_redemptions(exhibit_html, filing_info.filing_date)
            for rec in records:
                existing = seen_dates.get(rec.as_of_date)
                if existing is None:
                    seen_dates[rec.as_of_date] = rec
                else:
                    # Keep the record with more data filled in
                    fields = ["shares_tendered", "shares_redeemed", "value_redeemed"]
                    new_count = sum(1 for f in fields if getattr(rec, f) is not None)
                    old_count = sum(1 for f in fields if getattr(existing, f) is not None)
                    if new_count > old_count:
                        seen_dates[rec.as_of_date] = rec
        except Exception as e:
            logger.warning(f"Failed to parse exhibit {exhibit_name}: {e}")

    return list(seen_dates.values())


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
            stmt = sqlite_insert(NavPerShare).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                nav_per_share=rec.nav_per_share,
            ).on_conflict_do_update(
                index_elements=["fund_id", "as_of_date", "share_class"],
                set_=dict(
                    filing_id=filing_id,
                    nav_per_share=rec.nav_per_share,
                ),
            )
            await session.execute(stmt)

        # Distribution records
        for rec in parsed.distribution_records:
            stmt = sqlite_insert(Distribution).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                distribution_per_share=rec.distribution_per_share,
            ).on_conflict_do_update(
                index_elements=["fund_id", "as_of_date", "share_class"],
                set_=dict(
                    filing_id=filing_id,
                    distribution_per_share=rec.distribution_per_share,
                ),
            )
            await session.execute(stmt)

        # Shares issued records
        for rec in parsed.shares_issued_records:
            stmt = sqlite_insert(SharesIssued).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                share_class=rec.share_class,
                offering_type=rec.offering_type,
                cumulative_shares=rec.cumulative_shares,
                cumulative_consideration=rec.cumulative_consideration,
            ).on_conflict_do_update(
                index_elements=["fund_id", "as_of_date", "share_class", "offering_type"],
                set_=dict(
                    filing_id=filing_id,
                    cumulative_shares=rec.cumulative_shares,
                    cumulative_consideration=rec.cumulative_consideration,
                ),
            )
            await session.execute(stmt)

        # Resolve pct_tendered_of_os → shares_tendered using shares outstanding
        for rec in parsed.redemption_records:
            if rec.pct_tendered_of_os is not None and rec.shares_tendered is None:
                so_row = await session.execute(
                    text("""
                        SELECT total_shares_outstanding FROM shares_outstanding
                        WHERE fund_id = :fid AND as_of_date <= :dt
                        ORDER BY as_of_date DESC LIMIT 1
                    """),
                    {"fid": fund_id, "dt": str(rec.as_of_date)},
                )
                so = so_row.scalar()
                if so:
                    rec.shares_tendered = (
                        rec.pct_tendered_of_os / Decimal("100") * Decimal(str(so))
                    ).quantize(Decimal("1"))

        # Redemption records — 8-K data is preliminary and should not
        # overwrite authoritative SC TO-I/A data
        for rec in parsed.redemption_records:
            if rec.source_form_type == "8-K":
                # Check if an SC TO-I/A record already exists for this date
                existing = await session.execute(
                    text("""
                        SELECT source_form_type, shares_tendered, shares_redeemed, value_redeemed
                        FROM redemptions WHERE fund_id = :fid AND as_of_date = :dt
                    """),
                    {"fid": fund_id, "dt": str(rec.as_of_date)},
                )
                existing_row = existing.fetchone()

                if existing_row and existing_row[0] != "8-K":
                    # SC TO-I/A record exists — only fill in NULL fields
                    updates = {}
                    if existing_row[1] is None and rec.shares_tendered is not None:
                        updates["shares_tendered"] = rec.shares_tendered
                    if existing_row[2] is None and rec.shares_redeemed is not None:
                        updates["shares_redeemed"] = rec.shares_redeemed
                    if existing_row[3] is None and rec.value_redeemed is not None:
                        updates["value_redeemed"] = rec.value_redeemed
                    if updates:
                        set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
                        await session.execute(
                            text(f"UPDATE redemptions SET {set_clauses} WHERE fund_id = :fid AND as_of_date = :dt"),
                            {"fid": fund_id, "dt": str(rec.as_of_date), **updates},
                        )
                    continue

                # No record or existing 8-K record — upsert, filling in blanks
                stmt = sqlite_insert(Redemption).values(
                    fund_id=fund_id,
                    filing_id=filing_id,
                    as_of_date=rec.as_of_date,
                    shares_tendered=rec.shares_tendered,
                    shares_redeemed=rec.shares_redeemed,
                    value_redeemed=rec.value_redeemed,
                    source_form_type=rec.source_form_type,
                ).on_conflict_do_update(
                    index_elements=["fund_id", "as_of_date"],
                    set_=dict(
                        filing_id=filing_id,
                        shares_tendered=text("COALESCE(excluded.shares_tendered, redemptions.shares_tendered)"),
                        shares_redeemed=text("COALESCE(excluded.shares_redeemed, redemptions.shares_redeemed)"),
                        value_redeemed=text("COALESCE(excluded.value_redeemed, redemptions.value_redeemed)"),
                    ),
                )
            else:
                # SC TO-I/A and other authoritative sources always overwrite
                stmt = sqlite_insert(Redemption).values(
                    fund_id=fund_id,
                    filing_id=filing_id,
                    as_of_date=rec.as_of_date,
                    shares_tendered=rec.shares_tendered,
                    shares_redeemed=rec.shares_redeemed,
                    value_redeemed=rec.value_redeemed,
                    source_form_type=rec.source_form_type,
                ).on_conflict_do_update(
                    index_elements=["fund_id", "as_of_date"],
                    set_=dict(
                        filing_id=filing_id,
                        shares_tendered=rec.shares_tendered,
                        shares_redeemed=rec.shares_redeemed,
                        value_redeemed=rec.value_redeemed,
                        source_form_type=rec.source_form_type,
                    ),
                )
            await session.execute(stmt)

        # Total NAV records
        for rec in parsed.total_nav_records:
            stmt = sqlite_insert(TotalNav).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                total_nav=rec.total_nav,
            ).on_conflict_do_update(
                index_elements=["fund_id", "as_of_date"],
                set_=dict(
                    filing_id=filing_id,
                    total_nav=rec.total_nav,
                ),
            )
            await session.execute(stmt)

        # Shares outstanding records
        for rec in parsed.shares_outstanding_records:
            stmt = sqlite_insert(SharesOutstanding).values(
                fund_id=fund_id,
                filing_id=filing_id,
                as_of_date=rec.as_of_date,
                total_shares_outstanding=rec.total_shares_outstanding,
            ).on_conflict_do_update(
                index_elements=["fund_id", "as_of_date"],
                set_=dict(
                    filing_id=filing_id,
                    total_shares_outstanding=rec.total_shares_outstanding,
                ),
            )
            await session.execute(stmt)

        await session.commit()


async def backfill_redemption_values():
    """Fill in missing redemption value_redeemed by calculating NAV × shares.

    For redemptions where shares_redeemed is known but value_redeemed is NULL,
    compute value as the average NAV per share on the as_of_date × shares_redeemed.
    Uses the closest available NAV date if exact match not found.
    """
    async with async_session_factory() as session:
        # Find redemptions with shares but no value
        result = await session.execute(text("""
            SELECT r.id, r.fund_id, r.as_of_date, r.shares_redeemed
            FROM redemptions r
            WHERE r.shares_redeemed IS NOT NULL AND r.value_redeemed IS NULL
        """))
        missing = result.fetchall()

        if not missing:
            return 0

        filled = 0
        for row in missing:
            r_id, fund_id, as_of_date, shares = row

            # Get average NAV per share for this fund on/near this date
            # Try exact date first, then closest date within 60 days
            nav_result = await session.execute(text("""
                SELECT AVG(nav_per_share) FROM nav_per_share
                WHERE fund_id = :fund_id
                  AND ABS(JULIANDAY(as_of_date) - JULIANDAY(:as_of)) <= 60
                  AND nav_per_share IS NOT NULL
                ORDER BY ABS(JULIANDAY(as_of_date) - JULIANDAY(:as_of))
                LIMIT 20
            """), {"fund_id": fund_id, "as_of": str(as_of_date)})
            avg_nav = nav_result.scalar()

            if avg_nav and shares:
                value = Decimal(str(avg_nav)) * Decimal(str(shares))
                await session.execute(text("""
                    UPDATE redemptions SET value_redeemed = :value WHERE id = :id
                """), {"value": float(value), "id": r_id})
                filled += 1

        await session.commit()
        if filled:
            logger.info(f"Backfilled {filled} redemption values using NAV × shares")
        return filled


async def backfill_8k_exhibit_redemptions():
    """Re-check existing 8-K filings for tender/redemption data in exhibits.

    Scans all 8-K filings in the database, checks if their primary document
    references Exhibit 99.x, fetches and parses those exhibits, and stores
    any new redemption data found.
    """
    client = EdgarClient()
    try:
        async with async_session_factory() as session:
            result = await session.execute(text("""
                SELECT fi.id, fi.fund_id, fi.accession_number, fi.filing_date,
                       fi.raw_html, f.cik, f.ticker
                FROM filings fi
                JOIN funds f ON fi.fund_id = f.id
                WHERE fi.form_type = '8-K' AND fi.raw_html IS NOT NULL
                ORDER BY fi.filing_date
            """))
            filings = result.fetchall()

        found = 0
        for row in filings:
            filing_id, fund_id, accession, filing_date_str, raw_html, cik, ticker = row
            filing_date = date.fromisoformat(str(filing_date_str))

            records = []

            # Check primary document for tender data
            try:
                primary_records = parse_8k_exhibit_for_redemptions(raw_html, filing_date)
                records.extend(primary_records)
            except Exception as e:
                logger.warning(f"Backfill primary parse error for {ticker} {accession}: {e}")

            # Check exhibits if primary doc references them
            if has_tender_exhibit_references(raw_html):
                filing_info = FilingInfo(
                    accession_number=accession,
                    form_type="8-K",
                    filing_date=filing_date,
                    primary_document="",
                )
                try:
                    exhibit_records = await _fetch_and_parse_8k_exhibits(
                        client, cik, filing_info,
                    )
                    records.extend(exhibit_records)
                except Exception as e:
                    logger.warning(f"Backfill exhibit error for {ticker} {accession}: {e}")

            if records:
                # Deduplicate by as_of_date
                seen = {}
                for rec in records:
                    existing = seen.get(rec.as_of_date)
                    if existing is None:
                        seen[rec.as_of_date] = rec
                    else:
                        fields = ["shares_tendered", "shares_redeemed", "value_redeemed"]
                        new_count = sum(1 for f in fields if getattr(rec, f) is not None)
                        old_count = sum(1 for f in fields if getattr(existing, f) is not None)
                        if new_count > old_count:
                            seen[rec.as_of_date] = rec
                deduped = list(seen.values())

                parsed = ParsedFiling()
                parsed.redemption_records.extend(deduped)
                await _store_parsed_data(fund_id, filing_id, parsed)
                found += len(deduped)
                logger.info(
                    f"Backfill: found {len(deduped)} redemption(s) "
                    f"in 8-K for {ticker} ({filing_date})"
                )

        if found:
            logger.info(f"Backfilled {found} redemption records from 8-K exhibits")
        return found
    finally:
        await client.close()


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
