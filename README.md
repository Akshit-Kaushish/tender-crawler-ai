# TenderCrawler AI

TenderCrawler AI is a distributed, session-aware tender crawler for public procurement portals and protected bid sites. It combines FastAPI, Redis, Playwright, `httpx`, and OpenAI extraction to crawl only procurement-relevant pages, reuse active sessions when available, and extract multilingual tender data into structured JSON.

## What It Does

- Crawls tender, bid, RFP, RFQ, procurement, and contract-notice pages across many websites.
- Supports protected sources behind login.
- Reuses active sessions from Redis when available.
- Falls back to Azure Key Vault credentials when a fresh login is needed.
- Avoids full-site crawling by prioritizing procurement-like and pagination links.
- Extracts tender data in any language with AI-based classification and extraction.
- Ships with a responsive dashboard for crawl control, source onboarding, logs, and results preview.

## Architecture

```text
Dashboard UI / REST API (FastAPI)
            |
            v
         Redis
    - pending queue
    - processing set
    - results list
    - log stream
    - active sessions
    - protected source registry
            |
            v
         Workers
    - httpx fast fetch
    - Playwright JS/auth fetch
    - HTML cleaner
    - procurement-page classifier
    - AI tender extractor
            |
            v
 Azure Blob Storage + JSONL exports
 Azure Key Vault for credentials
```

## Main Features

### 1. Protected-source crawling

You can register a source in the dashboard or by API with:

- source name
- base URL
- auth method
- login URL
- username/password or cookie details
- form selectors where needed

Credentials are stored in Azure Key Vault. Live sessions are stored in Redis with TTL. When a crawl starts:

1. The input URL is matched to a saved source by domain.
2. If an active session exists, the crawler reuses it.
3. If no active session exists, the crawler logs in using Key Vault credentials.
4. The new session is cached and reused for later requests.

Supported auth flows:

- `form_login`
- `token_login`
- `basic_auth`
- `cookie_inject`

### 2. Fast, scoped crawling

The crawler is optimized to avoid wasting time on unrelated pages.

- `httpx` is attempted first for speed.
- Playwright is used only when rendering/auth is needed.
- Same-domain links are scored, ranked, and capped instead of following everything.
- Pagination links are preserved so tender listings can continue across pages.
- Duplicate queueing is prevented in Redis.
- Per-domain throttling avoids hammering a site.

### 3. AI extraction without turning AI into the bottleneck

The extraction pipeline is designed to stay accurate while controlling cost and latency.

- Cheap multilingual keyword checks run first.
- URL heuristics catch procurement portals even when HTML is thin.
- A small model is used as a classifier before full extraction.
- Large pages are chunked instead of truncated.
- AI concurrency is capped separately from worker concurrency.
- Extraction results are deduplicated across chunks.

### 4. Responsive dashboard

The dashboard includes:

- crawl launcher
- live status
- queue/result metrics
- live logs over SSE
- latest tender preview
- protected-source registration form
- active-session awareness per source
- URL-to-source resolution before crawl

## Project Structure

```text
api/
  main.py
crawler/
  ai_extractor.py
  auth_fetch.py
  auth_manager.py
  cleaner.py
  fetcher.py
  redis_manager.py
  source_registry.py
  worker.py
dashboard/
  index.html
storage/
  blob_client.py
```

## Requirements

- Python 3.11+
- Redis
- Azure Key Vault
- Azure Blob Storage
- OpenAI API access
- Playwright browser dependencies

## Environment Variables

Copy `.env.example` to `.env` and set the values you need.

Important variables:

```env
OPENAI_API_KEY=your-openai-key
REDIS_URL=redis://redis:6379
WORKER_CONCURRENCY=5
MAX_DEPTH=3
DOMAIN_DELAY=0.5
OUTPUT_DIR=/app/output
AZURE_KEY_VAULT_URL=https://your-vault-name.vault.azure.net/
```

You may also need:

- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_MODEL_MINI`
- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_BLOB_CONTAINER`
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY`

## Installation

### Local Python setup

```bash
pip install -r requirements.txt
playwright install chromium
```

### Docker setup

```bash
docker-compose up --build
```

To scale workers:

```bash
docker-compose up --scale worker=6
```

## Running the App

Start the API and workers using your preferred process manager or Docker Compose.

Default endpoints:

- Dashboard: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/api/health`

## Dashboard Workflow

### Crawl a site

1. Paste one or more procurement URLs.
2. The dashboard checks whether a matching protected source exists.
3. If a live session exists, it will be reused.
4. Start the crawl.
5. Watch logs, queue stats, and extracted tenders in real time.

### Add a protected source

1. Open the protected-source form in the dashboard.
2. Enter the source name and base URL.
3. Choose the auth method.
4. Add login URL and credential details.
5. Save the source.

The backend stores credentials in Key Vault and keeps only active sessions in Redis.

## API Reference

### `POST /api/crawl`

Start a crawl.

```json
{
  "urls": ["https://example.gov/tenders", "https://portal.org/procurement"],
  "max_depth": 3,
  "max_workers": 8,
  "domain_delay": 0.3,
  "use_playwright": true
}
```

### `GET /api/status`

Returns session, queue, worker, and extraction status.

### `GET /api/results?page=1&page_size=50`

Returns paginated tender results.

### `GET /api/logs/stream`

Server-sent events log stream for the dashboard.

### `POST /api/pause`

Pause crawling.

### `POST /api/resume`

Resume crawling.

### `POST /api/stop`

Stop crawling while keeping collected data.

### `DELETE /api/reset`

Clear Redis state and trigger blob cleanup.

### `GET /api/sources`

List registered protected sources and whether they currently have an active session.

### `POST /api/sources`

Register or update a protected source.

Example payload:

```json
{
  "name": "Example Procurement Portal",
  "base_url": "https://portal.example.gov/tenders",
  "method": "form_login",
  "login_url": "https://portal.example.gov/login",
  "username": "my-user",
  "password": "my-password",
  "username_selector": "#username",
  "password_selector": "#password",
  "submit_selector": "button[type=submit]",
  "success_url_fragment": "dashboard",
  "session_ttl": 3600,
  "notes": "Main procurement portal"
}
```

### `GET /api/sources/resolve?url=...`

Checks whether a URL matches a saved protected source and whether an active session already exists.

### `GET /api/sources/{auth_id}/session`

Returns source metadata and session availability for that source.

### `GET /api/download/results`

Downloads extracted tenders as JSONL.

### `GET /api/download/failed`

Downloads failed links as JSONL.

## Output Format

Each extracted tender object follows this structure:

```json
{
  "title": "Supply of Laboratory Equipment",
  "reference_number": "NIT/2024/LAB/001",
  "description": "Procurement of spectrometers and microscopes...",
  "date_posted": "15 March 2024",
  "deadline": "30 April 2024, 3:00 PM IST",
  "organization": "National Institute of Technology, Delhi",
  "category": "Goods",
  "location": "New Delhi",
  "estimated_value": "4500000 INR",
  "contact": "procurement@example.org",
  "document_links": ["https://example.org/tender.pdf"],
  "status": "open",
  "additional_info": "Lot 1 only",
  "source_url": "https://example.org/tenders/active",
  "fetched_at": "2026-04-17 12:00:00 UTC"
}
```

## Crawl Behavior

### Link selection

The crawler does not intentionally scrape the whole website.

It prefers:

- procurement-like paths
- bid/tender-related paths
- listing pages
- pagination URLs

It skips:

- login/account pages
- news/blog/contact pages
- document URLs as crawl targets
- low-value structural pages

### Crawl depth

- Depth 0: seed URLs
- Depth 1: likely listing pages
- Depth 2: tender detail pages
- Depth 3+: limited follow-up pages such as corrigenda or related notices

### Language coverage

The classifier and extractor are built to work across languages, including procurement vocabulary beyond English.

## Performance Notes

For better throughput:

- keep `DOMAIN_DELAY` low but safe, for example `0.2` to `0.5`
- use `httpx + Playwright` mode unless you know the site is fully static
- register protected sources in advance so workers can reuse existing sessions
- scale worker replicas instead of only raising per-worker concurrency
- avoid setting worker concurrency much higher than the infrastructure can handle

## Security Notes

- Do not hardcode credentials in source files.
- Store protected-source credentials in Azure Key Vault.
- Redis holds active sessions, not your permanent credential store.
- The dashboard clears password/cookie fields after submission on the client side.
- Review CORS and deployment settings before exposing the API publicly.

## Troubleshooting

### No active session is found

- Check that the source domain matches the crawl URL domain.
- Confirm the source was saved successfully.
- Confirm Redis is reachable.
- Confirm session TTL is not too short.

### Login fails

- Verify `AZURE_KEY_VAULT_URL` is set correctly.
- Confirm the secret was written to Key Vault.
- Check selector values for `form_login`.
- Verify the login URL is correct and reachable.

### Crawl is too broad

- Lower `MAX_DEPTH`.
- Start from a more specific procurement URL.
- Tune link scoring rules in [crawler/cleaner.py](/c:/Users/ITCELL/OneDrive/Desktop/tender_ai_scraper/tender_crawler_auth/crawler/cleaner.py).

### Crawl is too slow

- Increase worker replicas.
- Confirm most pages are staying on `httpx` and not falling back to Playwright unnecessarily.
- Check if the target site is rate-limiting or heavily JS-driven.
- Check OpenAI latency and reduce page size where appropriate.

### No tenders extracted

- Inspect live logs.
- Check whether the page is actually reachable after login.
- Test the cleaned page content path in [crawler/ai_extractor.py](/c:/Users/ITCELL/OneDrive/Desktop/tender_ai_scraper/tender_crawler_auth/crawler/ai_extractor.py).
- Review whether the site is serving challenge pages or captchas.

## Current Limitations

- Source matching is domain-based, not path-rule-based.
- Form-login flows assume stable selectors.
- Highly interactive portals may still need custom handling.
- Some sites with aggressive bot protection may require further browser hardening or proxy support.

## Next Good Improvements

- encrypted source metadata at rest in Redis
- source edit/delete APIs
- per-source crawl rules
- proxy rotation
- result export filters
- stronger test coverage for auth and crawl orchestration
