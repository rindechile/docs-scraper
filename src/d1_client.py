"""
Cloudflare D1 API client for database operations.
"""

import aiohttp
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Transient HTTP errors that should be retried
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class D1Client:
    """Client for Cloudflare D1 database operations via REST API"""

    def __init__(
        self,
        account_id: str,
        api_token: str,
        database_id: str,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        request_timeout: float = 30.0
    ):
        self.account_id = account_id
        self.api_token = api_token
        self.database_id = database_id
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.request_timeout = request_timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create a reusable HTTP session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        """Close the HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _execute(self, sql: str, params: list = None) -> Dict[str, Any]:
        """Execute a SQL query with retry logic for transient errors"""
        session = await self._get_session()
        payload = {"sql": sql}
        if params:
            payload["params"] = params

        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                async with session.post(
                    f"{self.base_url}/query",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout)
                ) as response:
                    
                    # Check for retryable HTTP errors BEFORE parsing JSON
                    if response.status in RETRYABLE_STATUS_CODES:
                        # Respect Retry-After header if present
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                        else:
                            delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                        
                        logger.warning(
                            f"D1 API returned {response.status}, retrying in {delay:.1f}s "
                            f"(attempt {attempt + 1}/{self.max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    
                    # Check for other non-success status codes
                    if response.status >= 400:
                        body_preview = (await response.text())[:200]
                        raise Exception(
                            f"D1 API error: HTTP {response.status}. Body: {body_preview}"
                        )
                    
                    # Check content type before parsing JSON
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' not in content_type:
                        body_preview = (await response.text())[:200]
                        raise Exception(
                            f"D1 API returned unexpected content type: {content_type}. "
                            f"Status: {response.status}. Body: {body_preview}"
                        )
                    
                    result = await response.json()

                    if not result.get("success"):
                        errors = result.get("errors", [])
                        raise Exception(f"D1 query failed: {errors}")

                    return result.get("result", [{}])[0]

            except aiohttp.ClientError as e:
                last_error = e
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                logger.warning(
                    f"D1 API connection error: {e}, retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                await asyncio.sleep(delay)
            
            except asyncio.TimeoutError:
                last_error = TimeoutError("D1 API request timed out")
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                logger.warning(
                    f"D1 API timeout, retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                await asyncio.sleep(delay)

        # All retries exhausted
        raise Exception(f"D1 API failed after {self.max_retries} attempts: {last_error}")

    async def get_pending_scrapes(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Get document scrapes that need processing"""
        result = await self._execute(
            """
            SELECT id, purchase_id, chilecompra_code, detail_url
            FROM document_scrapes
            WHERE scrape_status = 'pending'
            AND scrape_attempts < 3
            ORDER BY created_at ASC
            LIMIT ?
            """,
            [limit]
        )
        return result.get("results", [])

    async def get_scrapes_by_codes(self, codes: List[str]) -> List[Dict[str, Any]]:
        """Get specific document scrapes by chilecompra code"""
        placeholders = ",".join(["?" for _ in codes])
        result = await self._execute(
            f"""
            SELECT id, purchase_id, chilecompra_code, detail_url
            FROM document_scrapes
            WHERE chilecompra_code IN ({placeholders})
            """,
            codes
        )
        return result.get("results", [])

    async def update_status(self, scrape_ids: List[int], status: str):
        """Update scrape_status for multiple document scrapes (batched to avoid SQL variable limits)"""
        # SQLite has a limit of ~999 variables, so we batch in chunks of 50
        BATCH_SIZE = 50
        timestamp = datetime.now(timezone.utc).isoformat()

        for i in range(0, len(scrape_ids), BATCH_SIZE):
            batch = scrape_ids[i:i + BATCH_SIZE]
            placeholders = ",".join(["?" for _ in batch])
            await self._execute(
                f"""
                UPDATE document_scrapes
                SET scrape_status = ?,
                    updated_at = ?
                WHERE id IN ({placeholders})
                """,
                [status, timestamp] + batch
            )

    async def update_scrape_success(
        self,
        scrape_id: int,
        r2_folder: str,
        attachment_count: int,
        total_file_size: int,
        pdf_report_url: Optional[str] = None
    ):
        """Mark document scrape as successfully completed"""
        await self._execute(
            """
            UPDATE document_scrapes
            SET scrape_status = 'scraped',
                r2_folder = ?,
                attachment_count = ?,
                total_file_size = ?,
                pdf_report_url = ?,
                last_scrape_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            [
                r2_folder,
                attachment_count,
                total_file_size,
                pdf_report_url,
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                scrape_id
            ]
        )

    async def update_scrape_failed(self, scrape_id: int, error: str):
        """Mark document scrape as failed"""
        await self._execute(
            """
            UPDATE document_scrapes
            SET scrape_status = 'failed',
                scrape_error = ?,
                scrape_attempts = scrape_attempts + 1,
                last_scrape_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            [
                error[:500],  # Truncate long errors
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                scrape_id
            ]
        )

    async def insert_attachment(
        self,
        document_scrape_id: int,
        filename: str,
        file_type: Optional[str],
        r2_key: str,
        file_size: int,
        content_type: Optional[str] = None
    ):
        """Insert an attachment record"""
        await self._execute(
            """
            INSERT INTO attachments (document_scrape_id, filename, file_type, r2_key, file_size, content_type)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [document_scrape_id, filename, file_type, r2_key, file_size, content_type]
        )
