"""Rate-limited async HTTP client for SEC EDGAR APIs."""

import asyncio
import logging
import time
from typing import Any

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
EFTS_BASE = "https://efts.sec.gov/LATEST"


class EdgarClient:
    """Async HTTP client with rate limiting for SEC EDGAR."""

    def __init__(self):
        self._min_interval = 1.0 / settings.edgar_rate_limit
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers={
                    "User-Agent": settings.edgar_user_agent,
                    "Accept-Encoding": "gzip, deflate",
                },
                timeout=30.0,
                follow_redirects=True,
            )
        return self._client

    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_time = time.monotonic()

    async def _get(self, url: str, retries: int = 3) -> httpx.Response:
        """Make a rate-limited GET request with retry logic."""
        client = await self._get_client()
        for attempt in range(retries):
            await self._rate_limit()
            try:
                response = await client.get(url)
                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited by EDGAR, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (500, 502, 503, 504) and attempt < retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"EDGAR returned {e.response.status_code}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise
            except httpx.ProxyError as e:
                logger.error(f"Proxy error accessing {url}: {e}")
                raise RuntimeError(
                    f"Proxy error accessing SEC EDGAR. Ensure the application "
                    f"has direct internet access to data.sec.gov and www.sec.gov. "
                    f"Original error: {e}"
                ) from e
            except httpx.RequestError as e:
                if attempt < retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Request error: {e}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise
        raise RuntimeError(f"Failed to fetch {url} after {retries} retries")

    async def get_submissions(self, cik: str) -> dict[str, Any]:
        """Fetch the filing index for a company by CIK.

        Returns the full JSON response from data.sec.gov/submissions/CIK{padded}.json
        which includes company info and recent filings.
        """
        padded_cik = cik.zfill(10)
        url = f"{SUBMISSIONS_BASE}/CIK{padded_cik}.json"
        logger.info(f"Fetching submissions for CIK {cik}")
        response = await self._get(url)
        return response.json()

    async def get_filing_document(self, cik: str, accession_number: str, document: str) -> str:
        """Fetch a specific filing document's HTML content.

        Args:
            cik: Company CIK number
            accession_number: Filing accession number (with dashes)
            document: Primary document filename
        """
        # Accession number without dashes for the URL path
        accession_no_dashes = accession_number.replace("-", "")
        url = f"{ARCHIVES_BASE}/{cik}/{accession_no_dashes}/{document}"
        logger.info(f"Fetching document: {url}")
        response = await self._get(url)
        return response.text

    async def get_filing_index(self, cik: str, accession_number: str) -> dict[str, Any]:
        """Fetch the filing index JSON for a specific filing."""
        accession_no_dashes = accession_number.replace("-", "")
        url = f"{ARCHIVES_BASE}/{cik}/{accession_no_dashes}/index.json"
        response = await self._get(url)
        return response.json()

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
