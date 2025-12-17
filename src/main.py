"""
Main entry point for the MercadoPublico document scraper.
Designed to run in GitHub Actions with batched processing.
"""

import asyncio
import logging
import os
import json
import sys
from datetime import datetime, timezone

from .mercadopublico import MercadoPublicoScraper
from .d1_client import D1Client
from .r2_client import R2Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    def __init__(self):
        # Load configuration from environment
        self.batch_size = int(os.environ.get('BATCH_SIZE', 200))
        self.dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'
        self.max_concurrent = int(os.environ.get('MAX_CONCURRENT', 5))
        self.delay_min = float(os.environ.get('DELAY_MIN', 1.5))
        self.delay_max = float(os.environ.get('DELAY_MAX', 3.0))

        # Initialize clients
        self.d1 = D1Client(
            account_id=os.environ['CF_ACCOUNT_ID'],
            api_token=os.environ['CF_API_TOKEN'],
            database_id=os.environ['D1_DATABASE_ID']
        )

        self.r2 = R2Client(
            account_id=os.environ['CF_ACCOUNT_ID'],
            access_key=os.environ['R2_ACCESS_KEY'],
            secret_key=os.environ['R2_SECRET_KEY'],
            bucket=os.environ['R2_BUCKET']
        )

        self.scraper = MercadoPublicoScraper(
            max_concurrent=self.max_concurrent,
            delay_range=(self.delay_min, self.delay_max)
        )

        # Stats tracking
        self.stats = {
            'started_at': datetime.now(timezone.utc).isoformat(),
            'batch_size': self.batch_size,
            'processed': 0,
            'succeeded': 0,
            'failed': 0,
            'files_uploaded': 0,
            'total_bytes': 0
        }

    async def run(self, test_ids: list = None):
        """Main execution flow"""
        logger.info(f"Starting scraper (batch_size={self.batch_size}, dry_run={self.dry_run})")

        try:
            # Step 1: Get pending scrapes from D1
            if test_ids:
                scrapes = await self.d1.get_scrapes_by_codes(test_ids)
            else:
                scrapes = await self.d1.get_pending_scrapes(limit=self.batch_size)

            logger.info(f"Found {len(scrapes)} scrapes to process")

            if not scrapes:
                logger.info("No pending scrapes found")
                return

            # Step 2: Mark as "scraping" in database
            scrape_ids = [s['id'] for s in scrapes]
            await self.d1.update_status(scrape_ids, 'scraping')

            # Step 3: Process each scrape
            for scrape in scrapes:
                await self.process_scrape(scrape)
                self.stats['processed'] += 1

                # Log progress every 10 items
                if self.stats['processed'] % 10 == 0:
                    logger.info(f"Progress: {self.stats['processed']}/{len(scrapes)}")

            # Step 4: Save final stats
            self.stats['completed_at'] = datetime.now(timezone.utc).isoformat()
            self.save_stats()

            logger.info(f"Completed: {self.stats['succeeded']} succeeded, {self.stats['failed']} failed")

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            raise

    async def process_scrape(self, scrape: dict):
        """Process a single document scrape"""
        scrape_id = scrape['id']
        chilecompra_code = scrape['chilecompra_code']
        detail_url = scrape['detail_url']

        logger.info(f"Processing {chilecompra_code}")

        try:
            # Step 1: Scrape the purchase order
            result = await self.scraper.scrape_purchase(
                chilecompra_code=chilecompra_code,
                detail_url=detail_url
            )

            if not result['success']:
                raise Exception(result.get('error', 'Unknown scraping error'))

            # Step 2: Upload files to R2 (unless dry run)
            if not self.dry_run:
                r2_folder = f"raw/{chilecompra_code}/"
                uploaded_files = []
                total_bytes = 0

                # Upload main PDF report
                if result.get('pdf_report'):
                    key = f"{r2_folder}purchase_order.pdf"
                    await self.r2.upload_bytes(
                        key=key,
                        data=result['pdf_report'],
                        content_type='application/pdf'
                    )
                    uploaded_files.append(key)
                    total_bytes += len(result['pdf_report'])

                # Upload attachments
                for attachment in result.get('attachments', []):
                    if attachment.get('content'):
                        filename = self.sanitize_filename(attachment['filename'])
                        key = f"{r2_folder}{filename}"
                        await self.r2.upload_bytes(
                            key=key,
                            data=attachment['content'],
                            content_type=attachment.get('content_type', 'application/octet-stream')
                        )
                        uploaded_files.append(key)
                        total_bytes += len(attachment['content'])

                        # Record attachment in D1
                        await self.d1.insert_attachment(
                            document_scrape_id=scrape_id,
                            filename=attachment['filename'],
                            file_type=attachment.get('file_type'),
                            r2_key=key,
                            file_size=len(attachment['content']),
                            content_type=attachment.get('content_type')
                        )

                # Upload metadata
                metadata = {
                    'chilecompra_code': chilecompra_code,
                    'scraped_at': datetime.now(timezone.utc).isoformat(),
                    'pdf_report_url': result.get('pdf_report_url'),
                    'attachment_count': len(result.get('attachments', [])),
                    'files': uploaded_files
                }
                await self.r2.upload_json(
                    key=f"{r2_folder}metadata.json",
                    data=metadata
                )

                self.stats['files_uploaded'] += len(uploaded_files)
                self.stats['total_bytes'] += total_bytes

            # Step 3: Update D1 with success
            await self.d1.update_scrape_success(
                scrape_id=scrape_id,
                r2_folder=f"raw/{chilecompra_code}/",
                attachment_count=len(result.get('attachments', [])),
                total_file_size=total_bytes if not self.dry_run else 0,
                pdf_report_url=result.get('pdf_report_url')
            )

            self.stats['succeeded'] += 1
            logger.info(f"[OK] {chilecompra_code}: {len(result.get('attachments', []))} attachments")

        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"[FAIL] {chilecompra_code}: {e}")

            # Update D1 with failure
            await self.d1.update_scrape_failed(
                scrape_id=scrape_id,
                error=str(e)
            )

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Remove invalid characters from filename"""
        import re
        return re.sub(r'[<>:"/\\|?*]', '_', filename)

    def save_stats(self):
        """Save stats to JSON file"""
        with open('scraper_state.json', 'w') as f:
            json.dump(self.stats, f, indent=2)


async def main():
    # Check for test IDs
    test_ids = None
    if '--test-ids' in sys.argv:
        idx = sys.argv.index('--test-ids')
        if idx + 1 < len(sys.argv):
            test_ids = [id.strip() for id in sys.argv[idx + 1].split(',')]

    # Also check environment variable
    if not test_ids and os.environ.get('CHILECOMPRA_CODES'):
        test_ids = [id.strip() for id in os.environ['CHILECOMPRA_CODES'].split(',')]

    orchestrator = ScraperOrchestrator()
    await orchestrator.run(test_ids=test_ids)


if __name__ == '__main__':
    asyncio.run(main())
