"""
Scraper for MercadoPublico.cl purchase orders and attachments.
"""

import asyncio
import aiohttp
import random
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Any, Tuple

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

    async def _post_form(
        self,
        session: aiohttp.ClientSession,
        url: str,
        form_data: Dict[str, str],
        binary: bool = True
    ) -> Optional[bytes | str]:
        """POST form data with retry logic (for ASP.NET postbacks)"""
        async with self.semaphore:
            await self._rate_limit()

            headers = {
                **self.headers,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': self.BASE_URL,
                'Referer': url,
            }

            for attempt in range(self.max_retries):
                try:
                    async with session.post(
                        url,
                        headers=headers,
                        data=form_data,
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:

                        if response.status == 200:
                            if binary:
                                return await response.read()
                            return await response.text()

                        elif response.status == 429:
                            wait = (2 ** attempt) * 10
                            logger.warning(f"Rate limited on POST, waiting {wait}s")
                            await asyncio.sleep(wait)

                        else:
                            logger.warning(f"HTTP {response.status} on POST: {url}")

                except asyncio.TimeoutError:
                    logger.warning(f"POST Timeout (attempt {attempt + 1}): {url}")
                except Exception as e:
                    logger.warning(f"POST Error (attempt {attempt + 1}): {e}")

                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

            return None

    def _extract_aspnet_form_fields(self, html: str) -> Dict[str, str]:
        """Extract ASP.NET hidden form fields (__VIEWSTATE, __EVENTVALIDATION, etc.)"""
        soup = BeautifulSoup(html, 'lxml')
        form_data = {}

        # Common ASP.NET hidden fields
        field_names = [
            '__VIEWSTATE',
            '__VIEWSTATEGENERATOR',
            '__EVENTVALIDATION',
            '__EVENTTARGET',
            '__EVENTARGUMENT',
            '__PREVIOUSPAGE',
            '__VIEWSTATEENCRYPTED',
        ]

        for name in field_names:
            field = soup.find('input', {'name': name})
            if field and field.get('value') is not None:
                form_data[name] = field.get('value', '')

        return form_data

    def _extract_qs_param(self, url: str) -> Optional[str]:
        """Extract the 'qs' parameter from URL"""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get('qs', [None])[0]

    def _normalize_url(self, url: str) -> str:
        """Normalize URL by resolving ../ and cleaning up the path using urljoin"""
        # Use a simpler approach: let the browser-style resolution handle it
        # by joining with the base URL
        parsed = urlparse(url)

        # If the path contains ../, we need to resolve it
        if '/../' in parsed.path or parsed.path.startswith('../'):
            # Split the path and resolve manually
            parts = parsed.path.split('/')
            resolved = []
            for part in parts:
                if part == '..':
                    if resolved and resolved[-1] != '':
                        resolved.pop()
                elif part == '.':
                    continue
                else:
                    resolved.append(part)
            normalized_path = '/'.join(resolved)
            # Ensure path starts with /
            if not normalized_path.startswith('/'):
                normalized_path = '/' + normalized_path

            if parsed.query:
                return f"{parsed.scheme}://{parsed.netloc}{normalized_path}?{parsed.query}"
            return f"{parsed.scheme}://{parsed.netloc}{normalized_path}"

        return url

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
                        # Handle relative paths - extract just the filename and qs parameter
                        # Pattern like: ../../../Portal/Modules/Site/AdvancedSearch/ViewAttachmentPurchaseOrder.aspx?qs=...
                        if 'ViewAttachmentPurchaseOrder.aspx' in relative_url:
                            # Extract just the query string part
                            qs_match = re.search(r'\?qs=([^&\s]+)', relative_url)
                            if qs_match:
                                qs_param = qs_match.group(1)
                                result['attachments_url'] = f"{self.BASE_URL}/Portal/Modules/Site/AdvancedSearch/ViewAttachmentPurchaseOrder.aspx?qs={qs_param}"
                            else:
                                result['attachments_url'] = f"{self.BASE_URL}/Portal/Modules/Site/AdvancedSearch/ViewAttachmentPurchaseOrder.aspx"
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

        # Normalize URLs to resolve any ../ paths
        if result['pdf_report_url']:
            result['pdf_report_url'] = self._normalize_url(result['pdf_report_url'])
        if result['attachments_url']:
            result['attachments_url'] = self._normalize_url(result['attachments_url'])

        # Log what we found
        logger.info(f"Parsed detail page - PDF: {result['pdf_report_url']}, Attachments: {result['attachments_url']}")

        return result

    def _parse_attachments_page(self, html: str, base_url: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """
        Parse ViewAttachmentPurchaseOrder.aspx to extract attachment info.

        Expected structure:
        | Nombre del Anexo | Tipo | Fecha | Ver |
        | COT_xxx.pdf | Cotizacion | 23-06-2025 | [input button] |

        The download is triggered by clicking input[type=image] buttons with names like
        'rptAttachment$ctl01$imgShow'. These trigger ASP.NET postbacks that download the file.

        Returns:
            Tuple of (attachments list, ASP.NET form fields dict)
        """
        soup = BeautifulSoup(html, 'lxml')
        attachments = []
        seen_buttons = set()  # Track seen postback buttons to avoid duplicates

        # Extract ASP.NET form fields for postback
        form_fields = self._extract_aspnet_form_fields(html)
        logger.debug(f"Extracted form fields: {list(form_fields.keys())}")

        # Log HTML snippet for debugging
        logger.debug(f"Attachments page HTML length: {len(html)}")

        # Find all tables (there might be nested tables)
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables in attachments page")

        for table in tables:
            rows = table.find_all('tr', recursive=False)  # Only direct children to avoid nested table dupes
            if not rows:
                # Try getting rows from tbody
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr', recursive=False)

            for row in rows:
                cells = row.find_all('td', recursive=False)
                if len(cells) >= 3:
                    # Get text from first cells
                    filename = cells[0].get_text(strip=True)
                    file_type = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                    date = cells[2].get_text(strip=True) if len(cells) > 2 else ''

                    # Skip header-like rows or empty rows
                    if not filename or filename.lower() in ['nombre del anexo', 'nombre', 'anexo']:
                        continue

                    # Validate filename looks like a file (has extension)
                    if '.' not in filename or len(filename) > 100:
                        continue

                    # Look for download link - could be <a>, onclick, or ASP.NET postback button
                    download_url = None
                    postback_button = None

                    # Method 1: Look for <a> tag with href
                    for cell in cells:
                        link = cell.find('a', href=True)
                        if link:
                            href = link.get('href', '')
                            if href and not href.startswith('javascript'):
                                download_url = urljoin(base_url, href)
                                break

                    # Method 2: Look for onclick handler on any element
                    if not download_url:
                        for cell in cells:
                            for elem in cell.find_all(attrs={'onclick': True}):
                                onclick = elem.get('onclick', '')
                                # Look for window.open or direct URL patterns
                                url_match = re.search(r"'(https?://[^']+)'", onclick)
                                if url_match:
                                    download_url = url_match.group(1)
                                    break
                                # Look for relative URL in onclick
                                url_match = re.search(r"'(/[^']+\.pdf[^']*)'", onclick, re.IGNORECASE)
                                if url_match:
                                    download_url = f"{self.BASE_URL}{url_match.group(1)}"
                                    break

                    # Method 3: For ASP.NET postback buttons, extract the button name
                    # The imgShow buttons trigger postbacks - we simulate form POST
                    if not download_url:
                        img_input = row.find('input', {'type': 'image'})
                        if img_input:
                            input_name = img_input.get('name', '')
                            input_id = img_input.get('id', '')
                            if input_name and ('imgShow' in input_name or 'imgShow' in input_id or 'Show' in input_name):
                                # Skip if we've already seen this button (duplicate from nested tables)
                                if input_name in seen_buttons:
                                    continue
                                postback_button = input_name
                                seen_buttons.add(input_name)
                                logger.debug(f"Found postback button: {input_name} for {filename}")

                    if download_url:
                        attachments.append({
                            'filename': filename,
                            'file_type': file_type,
                            'date': date,
                            'download_url': download_url,
                            'postback_button': None
                        })
                        logger.info(f"Found attachment with direct URL: {filename} -> {download_url}")
                    elif postback_button:
                        attachments.append({
                            'filename': filename,
                            'file_type': file_type,
                            'date': date,
                            'download_url': None,
                            'postback_button': postback_button
                        })
                        logger.info(f"Found attachment with postback: {filename} -> button={postback_button}")
                    elif filename and (filename.endswith('.pdf') or filename.endswith('.PDF')):
                        # Log that we found a PDF but couldn't get the download URL
                        logger.warning(f"Found attachment '{filename}' but no download URL or postback button")

        logger.info(f"Parsed {len(attachments)} attachments from page")
        return attachments, form_fields

    async def _download_via_postback(
        self,
        session: aiohttp.ClientSession,
        page_url: str,
        button_name: str,
        form_fields: Dict[str, str]
    ) -> Optional[bytes]:
        """
        Download a file by simulating ASP.NET postback button click.

        For input[type=image] buttons, we need to POST with:
        - All ASP.NET hidden fields (__VIEWSTATE, etc.)
        - The button name with .x and .y coordinates (simulating image click)
        """
        # Build form data for postback
        form_data = dict(form_fields)

        # For input[type=image], ASP.NET expects button_name.x and button_name.y
        # Simulate clicking at coordinates (10, 10)
        form_data[f"{button_name}.x"] = "10"
        form_data[f"{button_name}.y"] = "10"

        logger.debug(f"Posting to {page_url} with button {button_name}")

        content = await self._post_form(session, page_url, form_data, binary=True)

        if content:
            # Check if we got HTML back (error page) or actual file content
            # PDF files start with %PDF, images have specific signatures
            if content[:4] == b'%PDF' or content[:8] == b'\x89PNG\r\n\x1a\n' or content[:2] == b'\xff\xd8':
                return content
            # Check if it looks like HTML (error response)
            try:
                if b'<!DOCTYPE' in content[:100] or b'<html' in content[:100].lower():
                    logger.warning(f"Got HTML response instead of file for button {button_name}")
                    return None
            except:
                pass
            # Assume it's a valid file even if we can't identify the type
            return content

        return None

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
                    logger.info(f"Fetching attachments page: {parsed['attachments_url']}")
                    attachments_html = await self._fetch(session, parsed['attachments_url'])

                    if attachments_html:
                        attachment_list, form_fields = self._parse_attachments_page(attachments_html, parsed['attachments_url'])
                        logger.info(f"Found {len(attachment_list)} attachments to download")

                        # Step 4: Download each attachment
                        for att in attachment_list:
                            content = None

                            # Try direct URL first
                            if att.get('download_url'):
                                content = await self._fetch(
                                    session,
                                    att['download_url'],
                                    binary=True
                                )

                            # Try postback if we have a button and no direct URL
                            elif att.get('postback_button') and form_fields:
                                logger.info(f"Downloading '{att['filename']}' via postback button: {att['postback_button']}")
                                content = await self._download_via_postback(
                                    session,
                                    parsed['attachments_url'],
                                    att['postback_button'],
                                    form_fields
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
                                logger.info(f"Downloaded attachment: {att['filename']} ({len(content)} bytes)")
                            else:
                                logger.warning(f"Failed to download attachment: {att['filename']}")

                result['success'] = True

            except Exception as e:
                result['error'] = str(e)
                logger.error(f"Error scraping {chilecompra_code}: {e}")

        return result
