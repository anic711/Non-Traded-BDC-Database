#!/bin/bash
# One-command setup: starts PostgreSQL, runs migrations, loads all EDGAR data.
#
# Prerequisites:
#   - Docker installed (for PostgreSQL), OR a local PostgreSQL instance
#   - Python 3.11+ with pip
#   - Internet access to data.sec.gov and www.sec.gov
#
# Usage:
#   chmod +x scripts/setup_and_load.sh
#   ./scripts/setup_and_load.sh
#
# To load a single fund:
#   ./scripts/setup_and_load.sh --ticker BCRED

set -e
cd "$(dirname "$0")/.."

echo "=== Non-Traded BDC Database Setup ==="

# 1. Install Python dependencies
echo "[1/5] Installing Python dependencies..."
pip install -r requirements.txt --quiet

# 2. Set up .env if not exists
if [ ! -f .env ]; then
    echo "[2/5] Creating .env from .env.example..."
    cp .env.example .env
    echo ""
    echo "  ⚠️  IMPORTANT: Edit .env and set EDGAR_USER_AGENT to your real contact info."
    echo "  SEC requires a User-Agent header with your name and email."
    echo "  Example: EDGAR_USER_AGENT=MyCompany admin@mycompany.com"
    echo ""
    read -p "  Press Enter after editing .env, or Ctrl+C to abort..."
fi

# 3. Start PostgreSQL (try docker compose first, fall back to local)
echo "[3/5] Starting PostgreSQL..."
if command -v docker &>/dev/null; then
    docker compose up -d db
    echo "  Waiting for PostgreSQL to be ready..."
    sleep 5
    # Wait for health check
    for i in {1..30}; do
        if docker compose exec db pg_isready -U bdc_user -d bdc_metrics &>/dev/null; then
            echo "  PostgreSQL is ready."
            break
        fi
        sleep 1
    done
else
    echo "  Docker not found. Assuming local PostgreSQL is running."
    echo "  Make sure DATABASE_URL in .env points to your PostgreSQL instance."
fi

# 4. Run migrations
echo "[4/5] Running database migrations..."
PYTHONPATH=. alembic upgrade head

# 5. Load data from SEC EDGAR
echo "[5/5] Loading data from SEC EDGAR (this may take several minutes)..."
PYTHONPATH=. python scripts/load_data.py "$@"

echo ""
echo "=== Setup Complete ==="
echo "Start the app with: PYTHONPATH=. uvicorn src.api.app:app --port 8000"
echo "Then visit: http://localhost:8000/docs for the API documentation"
echo "Manual update: curl -X POST http://localhost:8000/api/update/trigger"
