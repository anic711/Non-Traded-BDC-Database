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

echo "Starting server on port ${PORT:-8000}..."
exec uvicorn src.api.app:app --host 0.0.0.0 --port "${PORT:-8000}"
