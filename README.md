# MercadoPublico Document Scraper

Automated scraper for Chilean public procurement documents from MercadoPublico.cl. Downloads PDF purchase orders and attachments, storing them in Cloudflare R2 with metadata in Cloudflare D1.

## Architecture

```
GitHub Actions (scheduled)
        │
        ▼
┌───────────────────┐
│  Python Scraper   │
│  - Fetch pages    │
│  - Parse HTML     │
│  - Download PDFs  │
└───────────────────┘
        │
        ▼
┌───────────────────┐     ┌───────────────────┐
│  Cloudflare D1    │     │  Cloudflare R2    │
│  - Scrape status  │     │  - PDF files      │
│  - Attachments    │     │  - Metadata JSON  │
└───────────────────┘     └───────────────────┘
```

## Setup

### 1. Create R2 Bucket

In your Cloudflare dashboard:
1. Go to R2 > Create bucket
2. Name: `rindechile-documents`
3. Generate R2 API credentials (Access Key ID + Secret Access Key)

### 2. Configure GitHub Secrets

Add these secrets to your GitHub repository:

| Secret | Description |
|--------|-------------|
| `CF_ACCOUNT_ID` | Your Cloudflare account ID |
| `CF_API_TOKEN` | API token with D1 read/write permissions |
| `D1_DATABASE_ID` | Your D1 database ID |
| `R2_ACCESS_KEY` | R2 access key ID |
| `R2_SECRET_KEY` | R2 secret access key |
| `R2_BUCKET` | Bucket name (e.g., `rindechile-documents`) |

### 3. Apply Database Migration

Run the Drizzle migration to create the `document_scrapes` and `attachments` tables:

```bash
cd ../website
pnpm drizzle-kit generate
pnpm drizzle-kit migrate
```

### 4. Seed the Document Scrapes Table

```bash
cd ../website
pnpm tsx scripts/seed-document-scrapes.ts
```

## Usage

### Scheduled Runs

The scraper runs automatically every 4 hours via GitHub Actions. Each run processes up to 200 documents.

### Manual Testing

Trigger a manual run from the Actions tab, or:

```bash
# Test specific codes
CHILECOMPRA_CODES="3707-351-AG25,3707-352-AG25" python -m src.main

# Dry run (no uploads)
DRY_RUN=true python -m src.main
```

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export CF_ACCOUNT_ID="your-account-id"
export CF_API_TOKEN="your-api-token"
export D1_DATABASE_ID="your-database-id"
export R2_ACCESS_KEY="your-access-key"
export R2_SECRET_KEY="your-secret-key"
export R2_BUCKET="rindechile-documents"

# Run with test codes
CHILECOMPRA_CODES="3707-351-AG25" python -m src.main
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `BATCH_SIZE` | 200 | Documents to process per run |
| `MAX_CONCURRENT` | 5 | Maximum concurrent requests |
| `DELAY_MIN` | 1.5 | Minimum delay between requests (seconds) |
| `DELAY_MAX` | 3.0 | Maximum delay between requests (seconds) |
| `DRY_RUN` | false | Skip uploads, only scrape |

## R2 Storage Structure

```
raw/{chilecompra_code}/
├── purchase_order.pdf     # Main PDF report
├── attachment_1.pdf       # First attachment
├── attachment_2.pdf       # Second attachment
└── metadata.json          # Scrape metadata
```

## Monitoring

### Check Progress

Query your D1 database:

```sql
-- Count by status
SELECT scrape_status, COUNT(*) as count
FROM document_scrapes
GROUP BY scrape_status;

-- Recent failures
SELECT chilecompra_code, scrape_error, last_scrape_at
FROM document_scrapes
WHERE scrape_status = 'failed'
ORDER BY last_scrape_at DESC
LIMIT 10;
```

### GitHub Actions Logs

```bash
# List recent runs
gh run list --workflow=scrape.yml

# View logs for a specific run
gh run view <run-id> --log
```

## Cost Estimate

| Component | Cost |
|-----------|------|
| GitHub Actions | $0 (2000 min/mo free) |
| Cloudflare R2 | ~$1.50/mo for 100GB |
| Cloudflare D1 | ~$0 (within free tier) |
| **Total** | **~$1.50/month** |
