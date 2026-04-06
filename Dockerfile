FROM python:3.11-slim

WORKDIR /app

# Install system deps for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway injects PORT; default to 8000
ENV PORT=8000

# Use persistent volume for SQLite at /data
ENV DATABASE_URL=sqlite+aiosqlite:////data/bdc_metrics.db
ENV DATABASE_URL_SYNC=sqlite:////data/bdc_metrics.db

EXPOSE ${PORT}

# Start script handles migration + optional initial load + server
CMD ["bash", "scripts/start.sh"]
