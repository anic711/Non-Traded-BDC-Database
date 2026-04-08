#!/bin/bash
# Railway entrypoint: migrate, optionally load data in background, start server.
set -e

echo "=== Starting Non-Traded BDC Metrics ==="

# Ensure data directory exists
mkdir -p /data

# Run alembic migrations
echo "Running migrations..."
PYTHONPATH=/app alembic upgrade head

# If database has no filings yet, kick off initial data load in background
FILING_COUNT=$(python -c "
import sqlite3, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if not os.path.exists(db):
    print(0)
else:
    conn = sqlite3.connect(db)
    print(conn.execute('SELECT COUNT(*) FROM filings').fetchone()[0])
" 2>/dev/null || echo 0)

if [ "$FILING_COUNT" -eq 0 ]; then
    echo "Fresh database — loading data from SEC EDGAR in background..."
    PYTHONPATH=/app python scripts/load_data.py &
else
    echo "Database has $FILING_COUNT filings — skipping initial load."
fi

# One-time fix: reparse HLEND filings that had wrong effective dates
python -c "
import sqlite3, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if os.path.exists(db):
    conn = sqlite3.connect(db)
    # Check if HLEND is missing Nov 2025 / Feb 2026 shares_issued
    hlend_id = conn.execute(\"SELECT id FROM funds WHERE ticker='HLEND'\").fetchone()
    if hlend_id:
        hlend_id = hlend_id[0]
        missing = conn.execute(
            'SELECT COUNT(*) FROM shares_issued WHERE fund_id=? AND as_of_date IN (\"2025-11-01\",\"2026-02-01\")',
            (hlend_id,)
        ).fetchone()[0]
        if missing == 0:
            # Reset these filings so pipeline will reparse them
            conn.execute(
                'UPDATE filings SET parse_status=\"pending\", parsed_at=NULL WHERE fund_id=? AND filing_date IN (\"2025-12-01\",\"2026-03-03\")',
                (hlend_id,)
            )
            conn.commit()
            print('Reset HLEND filings for reparse')
    conn.close()
" 2>/dev/null || true

# One-time fix: reparse BCRED Q1 2026 SC TO-I/A (offset split between redeemed/tendered)
python -c "
import sqlite3, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if os.path.exists(db):
    conn = sqlite3.connect(db)
    # Check if BCRED Q1 2026 still has shares_redeemed == shares_tendered (both 151M)
    row = conn.execute('''
        SELECT r.id, r.shares_redeemed, r.shares_tendered, f.id as fid
        FROM redemptions r
        JOIN filings f ON r.filing_id = f.id
        JOIN funds fu ON r.fund_id = fu.id
        WHERE fu.ticker = \"BCRED\" AND r.as_of_date = \"2026-03-31\"
          AND r.shares_redeemed IS NOT NULL AND r.shares_tendered IS NOT NULL
          AND ABS(r.shares_redeemed - r.shares_tendered) < 1
    ''').fetchone()
    if row:
        rid, sr, st, fid = row
        print(f'Resetting BCRED Q1 2026 for reparse (redeemed={sr} == tendered={st})')
        conn.execute('DELETE FROM redemptions WHERE id=?', (rid,))
        conn.execute('UPDATE filings SET parse_status=\"pending\", parsed_at=NULL WHERE id=?', (fid,))
        conn.commit()
    conn.close()
" 2>/dev/null || true

# One-time fix: reparse ALL SC TO-I/A filings to populate shares_tendered
# The parser now distinguishes "shares accepted" from "total tender requests"
python -c "
import sqlite3, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if os.path.exists(db):
    conn = sqlite3.connect(db)
    # Check if any SC TO-I/A redemption is missing shares_tendered (new field)
    missing = conn.execute('''
        SELECT COUNT(*)
        FROM redemptions r
        JOIN filings f ON r.filing_id = f.id
        WHERE f.form_type LIKE \"SC TO-I%\"
          AND r.shares_tendered IS NULL
          AND r.shares_redeemed IS NOT NULL
    ''').fetchone()[0]
    if missing > 0:
        print(f'Found {missing} SC TO-I/A redemptions missing shares_tendered, resetting for reparse')
        fids = conn.execute('''
            SELECT DISTINCT f.id
            FROM filings f
            WHERE f.form_type LIKE \"SC TO-I%\"
        ''').fetchall()
        for (fid,) in fids:
            conn.execute('DELETE FROM redemptions WHERE filing_id=?', (fid,))
            conn.execute('UPDATE filings SET parse_status=\"pending\", parsed_at=NULL WHERE id=?', (fid,))
        conn.commit()
        print(f'Reset {len(fids)} SC TO-I/A filings for reparse')
    conn.close()
" 2>/dev/null || true

# One-time fix: reparse SC TO-I/A filings with wrong as_of dates
# The parser now extracts offer expiration date snapped to quarter-end.
python -c "
import sqlite3, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if os.path.exists(db):
    conn = sqlite3.connect(db)
    # Check for redemptions with non-quarter-end dates (should all be quarter-ends)
    bad = conn.execute('''
        SELECT r.id, r.as_of_date, f.id as fid
        FROM redemptions r
        JOIN filings f ON r.filing_id = f.id
        WHERE f.form_type LIKE \"SC TO-I%\"
          AND substr(r.as_of_date, 9, 2) NOT IN (\"31\",\"30\")
    ''').fetchall()
    if bad:
        print(f'Found {len(bad)} redemptions with non-quarter-end dates, resetting filings for reparse')
        for rid, dt, fid in bad:
            conn.execute('DELETE FROM redemptions WHERE id=?', (rid,))
            conn.execute('UPDATE filings SET parse_status=\"pending\", parsed_at=NULL WHERE id=?', (fid,))
        conn.commit()
    conn.close()
" 2>/dev/null || true

# If there are pending filings from fixes above, reparse them
python -c "
import asyncio, sqlite3, os
from datetime import date, datetime, timezone

db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if not os.path.exists(db):
    exit(0)

conn = sqlite3.connect(db)
pending = conn.execute('''
    SELECT id, fund_id, form_type, filing_date, raw_html
    FROM filings WHERE parse_status=\"pending\" AND raw_html IS NOT NULL
''').fetchall()
conn.close()

if not pending:
    exit(0)

print(f'Reparsing {len(pending)} pending filings...')

from src.parsers.filing_sctoi import parse_sctoi
from src.parsers.filing_8k import parse_8k
from src.parsers.filing_10q10k import parse_10q10k
from src.database import async_session_factory
from src.collectors.pipeline import _store_parsed_data, _parse_filing
from src.collectors.edgar_types import FilingInfo
from sqlalchemy import text as sql_text
from src.database.models import Filing

async def reparse():
    for fid, fund_id, form_type, fdate_str, html in pending:
        fdate = date.fromisoformat(str(fdate_str))
        info = FilingInfo(accession_number='', form_type=form_type, filing_date=fdate, primary_document='')
        parsed = _parse_filing(html, info)
        if parsed and parsed.has_data:
            await _store_parsed_data(fund_id, fid, parsed)
            async with async_session_factory() as session:
                f = await session.get(Filing, fid)
                f.parse_status = 'success'
                f.parsed_at = datetime.now(timezone.utc)
                await session.commit()
            print(f'  Reparsed filing {fid} ({form_type}): success')
        else:
            async with async_session_factory() as session:
                f = await session.get(Filing, fid)
                f.parse_status = 'skipped'
                f.parsed_at = datetime.now(timezone.utc)
                await session.commit()
            print(f'  Reparsed filing {fid} ({form_type}): skipped')

asyncio.run(reparse())
" 2>/dev/null || true

# Re-run 8-K exhibit backfill to pick up pct_tendered → shares_tendered conversion
python -c "
import asyncio, os
db = os.environ.get('DATABASE_URL_SYNC','').replace('sqlite:///','')
if not os.path.exists(db):
    exit(0)

import sqlite3
conn = sqlite3.connect(db)
# Only run if there are 8-K redemptions missing shares_tendered
missing = conn.execute('''
    SELECT COUNT(*) FROM redemptions
    WHERE source_form_type = \"8-K\" AND shares_tendered IS NULL AND value_redeemed IS NOT NULL
''').fetchone()[0]
conn.close()

if missing == 0:
    exit(0)

print(f'Re-running 8-K exhibit backfill for {missing} records missing shares_tendered...')
from src.collectors.pipeline import backfill_8k_exhibit_redemptions
asyncio.run(backfill_8k_exhibit_redemptions())
" 2>/dev/null || true

echo "Starting server on port ${PORT:-8000}..."
exec uvicorn src.api.app:app --host 0.0.0.0 --port "${PORT:-8000}"
