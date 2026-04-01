#!/usr/bin/env python3
"""Standalone script to run the full data collection pipeline.

Run this script on a machine with direct internet access to sec.gov.
It will fetch all relevant filings from EDGAR and populate the database.

Usage:
    # Full update (all funds, all dates since Jan 2023)
    python scripts/load_data.py

    # Single fund
    python scripts/load_data.py --ticker BCRED

    # Custom date range
    python scripts/load_data.py --from-date 2024-01-01
"""

import argparse
import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.pipeline import run_update, _ensure_funds_seeded, _process_fund
from src.database import async_session_factory
from src.edgar.client import EdgarClient
from src.models import Fund
from src.config import DATA_START_DATE
from sqlalchemy import select
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Load BDC data from SEC EDGAR")
    parser.add_argument("--ticker", help="Only process this fund ticker")
    parser.add_argument("--from-date", default=DATA_START_DATE,
                        help="Start date (YYYY-MM-DD, default: 2023-01-01)")
    args = parser.parse_args()

    date_from = date.fromisoformat(args.from_date)

    # Ensure funds are in the database
    await _ensure_funds_seeded()

    # Get funds to process
    async with async_session_factory() as session:
        query = select(Fund).where(Fund.active.is_(True))
        if args.ticker:
            query = query.where(Fund.ticker == args.ticker.upper())
        result = await session.execute(query)
        funds = result.scalars().all()

    if not funds:
        logger.error(f"No funds found{' for ticker ' + args.ticker if args.ticker else ''}")
        return

    logger.info(f"Processing {len(funds)} fund(s): {', '.join(f.ticker for f in funds)}")
    logger.info(f"Date range: {date_from} to present")

    client = EdgarClient()
    total = 0
    try:
        for fund in funds:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {fund.ticker} ({fund.name})")
            logger.info(f"CIK: {fund.cik}")
            logger.info(f"{'='*60}")
            try:
                processed = await _process_fund(client, fund, date_from)
                total += processed
                logger.info(f"Processed {processed} new filings for {fund.ticker}")
            except Exception as e:
                logger.error(f"Error processing {fund.ticker}: {e}")
    finally:
        await client.close()

    logger.info(f"\nDone! Total filings processed: {total}")


if __name__ == "__main__":
    asyncio.run(main())
