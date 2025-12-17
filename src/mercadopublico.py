"""
Scraper for MercadoPublico.cl purchase orders and attachments.
"""

import asyncio
import aiohttp
import random
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)


class MercadoPublicoScraper:
    BASE_URL = "https://www.mercadopublico.cl"
    DETAIL_PATH = "/PurchaseOrder/Modules/PO/DetailsPurchaseOrder.aspx"
    ATTACHMENTS_PATH = "/Portal/Modules/Site/AdvancedSearch/ViewAttachmentPurchaseOrder.aspx"
    PDF_REPORT_PATH = "/PurchaseOrder/Modules/PO/PDFReport.aspx"

    def __init__(
        self,
        max_concurrent: int = 5,
        delay_range: tuple = (1.5, 3.0),
        max_retries: int = 3,
        timeout: int = 30
    ):
        self.max_concurrent = max_concurrent
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }

    async def _rate_limit(self):
        """Apply random delay between requests"""
        delay = random.uniform(*self.delay_range)
        await asyncio.sleep(delay)

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        binary: bool = False
    ) -> Optional[bytes | str]:
        """Fetch URL with retry logic"""
        async with self.semaphore:
            await self._rate_limit()

            for attempt in range(self.max_retries):
                try:
                    async with session.get(
                        url,
                        headers=self.headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:

                        if response.status == 200:
                            if binary:
                                return await response.read()
                            return await response.text()

                        elif response.status == 429:
                            # Rate limited
                            wait = (2 ** attempt) * 10
                            logger.warning(f"Rate limited, waiting {wait}s")
                            await asyncio.sleep(wait)

                        else:
                            logger.warning(f"HTTP {response.status}: {url}")

                except asyncio.TimeoutError:
                    logger.warning(f"Timeout (attempt {attempt + 1}): {url}")
                except Exception as e:
                    logger.warning(f"Error (attempt {attempt + 1}): {e}")

                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

            return None

    def _extract_qs_param(self, url: str) -> Optional[str]:
        """Extract the 'qs' parameter from URL"""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get('qs', [None])[0]

    def _parse_detail_page(self, html: str) -> Dict[str, Any]:
        """
        Parse DetailsPurchaseOrder.aspx to find:
        1. PDF Report link (from onclick handler)
        2. Attachments link (from onclick handler)
        """
        soup = BeautifulSoup(html, 'lxml')
        result = {
            'pdf_report_url': None,
            'attachments_url': None,
        }

        # Find all elements with onclick handlers
        for elem in soup.find_all(attrs={'onclick': True}):
            onclick = elem.get('onclick', '')

            # Look for PDF Report URL
            if 'PDFReport' in onclick:
                # Pattern: open('PDFReport.aspx?qs=...' or window.open('...')
                match = re.search(r"'([^']*PDFReport\.aspx\?qs=[^']+)'", onclick)
                if match:
                    relative_url = match.group(1)
                    if relative_url.startswith('http'):
                        result['pdf_report_url'] = relative_url
                    else:
                        result['pdf_report_url'] = f"{self.BASE_URL}/PurchaseOrder/Modules/PO/{relative_url}"
                    logger.info(f"Found PDF URL: {result['pdf_report_url']}")

            # Look for Attachments URL (ViewAttachmentPurchaseOrder)
            if 'ViewAttachmentPurchaseOrder' in onclick or 'Attachment' in onclick:
                match = re.search(r"'([^']*ViewAttachmentPurchaseOrder[^']+)'", onclick)
                if match:
                    relative_url = match.group(1)
                    if relative_url.startswith('http'):
                        result['attachments_url'] = relative_url
                    elif relative_url.startswith('/'):
                        result['attachments_url'] = f"{self.BASE_URL}{relative_url}"
                    else:
                        result['attachments_url'] = f"{self.BASE_URL}/Portal/Modules/Site/AdvancedSearch/{relative_url}"
                    logger.info(f"Found Attachments URL: {result['attachments_url']}")

        # Also check href attributes for links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')

            if 'PDFReport' in href and not result['pdf_report_url']:
                if href.startswith('http'):
                    result['pdf_report_url'] = href
                else:
                    result['pdf_report_url'] = urljoin(self.BASE_URL, href)

            if 'ViewAttachmentPurchaseOrder' in href and not result['attachments_url']:
                if href.startswith('http'):
                    result['attachments_url'] = href
                else:
                    result['attachments_url'] = urljoin(self.BASE_URL, href)

        # Log what we found
        logger.info(f"Parsed detail page - PDF: {result['pdf_report_url'] is not None}, Attachments: {result['attachments_url'] is not None}")

        return result

    def _parse_attachments_page(self, html: str) -> List[Dict[str, Any]]:
        """
        Parse ViewAttachmentPurchaseOrder.aspx to extract attachment info.

        Expected structure:
        | Nombre del Anexo | Tipo | Fecha | Ver |
        | COT_xxx.pdf | Cotizacion | 23-06-2025 | [link] |
        """
        soup = BeautifulSoup(html, 'lxml')
        attachments = []

        # Find the table
        table = soup.find('table')
        if not table:
            return attachments

        rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                filename = cells[0].get_text(strip=True)
                file_type = cells[1].get_text(strip=True)
                date = cells[2].get_text(strip=True)

                # Skip header-like rows
                if filename.lower() in ['nombre del anexo', 'nombre', '']:
                    continue

                # Find download link
                download_link = cells[3].find('a') or cells[-1].find('a')
                if download_link:
                    href = download_link.get('href', '')
                    if href:
                        download_url = urljoin(self.BASE_URL, href)
                        attachments.append({
                            'filename': filename,
                            'file_type': file_type,
                            'date': date,
                            'download_url': download_url
                        })

        return attachments

    async def scrape_purchase(
        self,
        chilecompra_code: str,
        detail_url: str
    ) -> Dict[str, Any]:
        """
        Complete scraping flow for a single purchase order.

        Returns:
            {
                'success': bool,
                'error': str (if failed),
                'pdf_report': bytes,
                'pdf_report_url': str,
                'attachments': [
                    {
                        'filename': str,
                        'file_type': str,
                        'content': bytes,
                        'content_type': str
                    }
                ]
            }
        """
        result = {
            'success': False,
            'pdf_report': None,
            'pdf_report_url': None,
            'attachments': []
        }

        async with aiohttp.ClientSession() as session:
            try:
                # Step 1: Fetch detail page to get PDF link
                logger.debug(f"Fetching detail page: {detail_url}")
                detail_html = await self._fetch(session, detail_url)
                if not detail_html:
                    result['error'] = "Failed to fetch detail page"
                    return result

                parsed = self._parse_detail_page(detail_html)
                result['pdf_report_url'] = parsed.get('pdf_report_url')

                # Step 2: Download PDF report
                if parsed.get('pdf_report_url'):
                    logger.debug(f"Downloading PDF report")
                    pdf_content = await self._fetch(
                        session,
                        parsed['pdf_report_url'],
                        binary=True
                    )
                    if pdf_content:
                        result['pdf_report'] = pdf_content

                # Step 3: Fetch attachments page
                if parsed.get('attachments_url'):
                    logger.debug(f"Fetching attachments page")
                    attachments_html = await self._fetch(session, parsed['attachments_url'])

                    if attachments_html:
                        attachment_list = self._parse_attachments_page(attachments_html)
                        logger.debug(f"Found {len(attachment_list)} attachments")

                        # Step 4: Download each attachment
                        for att in attachment_list:
                            content = await self._fetch(
                                session,
                                att['download_url'],
                                binary=True
                            )
                            if content:
                                # Detect content type
                                content_type = 'application/octet-stream'
                                if att['filename'].lower().endswith('.pdf'):
                                    content_type = 'application/pdf'
                                elif att['filename'].lower().endswith(('.jpg', '.jpeg')):
                                    content_type = 'image/jpeg'
                                elif att['filename'].lower().endswith('.png'):
                                    content_type = 'image/png'

                                result['attachments'].append({
                                    'filename': att['filename'],
                                    'file_type': att.get('file_type'),
                                    'date': att.get('date'),
                                    'content': content,
                                    'content_type': content_type
                                })

                result['success'] = True

            except Exception as e:
                result['error'] = str(e)
                logger.error(f"Error scraping {chilecompra_code}: {e}")

        return result
