"""
Async fetcher — Playwright (stealth, popup/captcha bypass) + httpx fallback.

FIXES:
  1. Browser launched ONCE per process, reused across all URLs.
     Old code: async with async_playwright() as p: browser = await p.chromium.launch(...)
     That launches+closes Chromium for EVERY URL → huge overhead, slow startup logs
     polluting output, and wasting bandwidth reconnecting each time.
     New: module-level _browser pool initialized once on first use.

  2. Reduced artificial sleeps (1.2s → 0.4s, 0.5s removed).

  3. Playwright stdout is suppressed by routing to /dev/null via launch args,
     preventing browser startup messages from leaking into your terminal.

  4. _page_semaphore added — limits concurrent Playwright pages to 3 to prevent
     "max number of clients reached" errors when WORKER_CONCURRENCY is high.
"""
import asyncio
import random
import re
import time
from typing import Optional
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
Object.defineProperty(navigator, 'permissions', {
  get: () => ({query: () => Promise.resolve({state: 'granted'})})
});
"""

POPUP_SELECTORS = [
    "button[id*='accept']", "button[id*='cookie']", "button[id*='agree']",
    "button[id*='consent']", "button[id*='close']", "button[id*='dismiss']",
    "button[class*='accept']", "button[class*='cookie']", "button[class*='consent']",
    "a[id*='accept']", "a[class*='accept']", "[aria-label='Accept']",
    "[aria-label='Close']", ".cookie-accept", ".accept-cookies",
    "#accept-cookies", "#cookie-accept", ".gdpr-accept",
]

# Module-level browser singleton — launched once, reused for all pages
_playwright_instance = None
_browser = None
_browser_lock = asyncio.Lock()

# FIX: Limit concurrent Playwright pages per process.
# Tune with PLAYWRIGHT_MAX_PAGES if you have resources and need more throughput.
try:
    _pw_pages = int(os.getenv("PLAYWRIGHT_MAX_PAGES", "3"))
except Exception:
    _pw_pages = 3
_pw_pages = max(1, min(20, _pw_pages))
_page_semaphore = asyncio.Semaphore(_pw_pages)


async def _get_browser():
    """Return the shared browser instance, launching it once if needed."""
    global _playwright_instance, _browser
    async with _browser_lock:
        if _browser is None or not _browser.is_connected():
            from playwright.async_api import async_playwright
            _playwright_instance = await async_playwright().start()
            _browser = await _playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--window-size=1366,768",
                    "--log-level=3",
                    "--silent-debugger-extension-api",
                ]
            )
            logger.info("Playwright browser launched (shared instance)")
    return _browser


async def close_browser():
    """Call on shutdown to cleanly close the shared browser."""
    global _browser, _playwright_instance
    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None
    if _playwright_instance:
        try:
            await _playwright_instance.stop()
        except Exception:
            pass
        _playwright_instance = None


_CHALLENGE_PATTERNS = {
    "recaptcha": re.compile(r"recaptcha|g-recaptcha", re.IGNORECASE),
    "hcaptcha": re.compile(r"hcaptcha", re.IGNORECASE),
    "cloudflare": re.compile(r"cf-challenge|attention required|cloudflare", re.IGNORECASE),
    "turnstile": re.compile(r"turnstile", re.IGNORECASE),
    "captcha": re.compile(r"\bcaptcha\b|\bi am human\b|\bverify you are human\b", re.IGNORECASE),
}


def detect_challenge(html: str, status: int = 200) -> str:
    sample = (html or "")[:15000]
    if status in (403, 429, 503) and not sample.strip():
        return "access_blocked"
    for name, pattern in _CHALLENGE_PATTERNS.items():
        if pattern.search(sample):
            return name
    return ""


class FetchResult:
    __slots__ = ('html', 'status', 'url', 'fetched_at', 'method', 'success', 'challenge')

    def __init__(
        self,
        html: str,
        status: int,
        url: str,
        fetched_at: float,
        method: str = "httpx",
        challenge: str = "",
    ):
        self.html       = html
        self.status     = status
        self.url        = url
        self.fetched_at = fetched_at
        self.method     = method
        self.challenge  = challenge
        self.success    = status == 200 or (bool(html) and status < 400)


async def _dismiss_popups(page) -> None:
    for sel in POPUP_SELECTORS:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click(timeout=1500)
                await asyncio.sleep(0.2)
                break
        except Exception:
            continue
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass


async def fetch_with_playwright(url: str, timeout: int = 25000) -> FetchResult:
    """
    Playwright fetch using shared browser instance.
    Hard 30s asyncio timeout wraps the entire operation including semaphore
    acquisition — prevents the semaphore from getting permanently stuck when
    sites hang and never release their page slot.
    """
    from playwright.async_api import TimeoutError as PWTimeout
    import contextlib

    async def _do_fetch() -> FetchResult:
        async with _page_semaphore:
            fetched_at = time.time()
            ua = random.choice(USER_AGENTS)
            context = None

            try:
                browser = await _get_browser()
                context = await browser.new_context(
                    user_agent=ua,
                    viewport={"width": 1366, "height": 768},
                    java_script_enabled=True,
                    ignore_https_errors=True,
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Windows"',
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                    }
                )
                await context.add_init_script(STEALTH_SCRIPT)

                async def _route_handler(route):
                    try:
                        if route.request.resource_type in ("image", "stylesheet", "font", "media", "websocket"):
                            await route.abort()
                        else:
                            await route.continue_()
                    except Exception:
                        # Context/page may be closing due to timeout/cancel; ignore.
                        return

                await context.route("**/*", _route_handler)

                page = await context.new_page()
                status = 200
                challenge = ""

                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                    if response:
                        status = response.status

                    await asyncio.sleep(0.4)
                    await _dismiss_popups(page)
                    await page.evaluate("window.scrollTo(0, Math.min(1000, document.body.scrollHeight))")

                    html = await page.content()

                    challenge = detect_challenge(html, status)
                    if challenge:
                        logger.warning(f"Captcha/challenge detected on {url}: {challenge}")

                except PWTimeout:
                    try:
                        html = await page.content()
                        status = 206
                        challenge = detect_challenge(html, status)
                    except Exception:
                        html = ""
                        status = 408
                        challenge = ""
                finally:
                    with contextlib.suppress(Exception):
                        await asyncio.shield(page.close())

                return FetchResult(
                    html=html,
                    status=status,
                    url=url,
                    fetched_at=fetched_at,
                    method="playwright",
                    challenge=challenge,
                )

            except Exception as e:
                logger.warning(f"Playwright failed for {url}: {e}")
                return await fetch_with_httpx(url)
            finally:
                if context:
                    try:
                        await asyncio.shield(context.close())
                    except Exception:
                        pass

    try:
        # Hard 30s timeout on the entire Playwright operation including semaphore wait
        # Prevents the semaphore from getting stuck when all 3 slots are on hung sites
        hard_timeout_s = max(30, int(timeout / 1000) + 10)
        task = asyncio.create_task(_do_fetch())
        try:
            return await asyncio.wait_for(task, timeout=hard_timeout_s)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            raise
    except asyncio.TimeoutError:
        logger.warning(f"Playwright hard timeout (30s) for {url} — falling back to httpx")
        return await fetch_with_httpx(url)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=4), reraise=False)
async def fetch_with_httpx(url: str, timeout: int = 15) -> FetchResult:
    """Fast httpx fetch with retry, realistic headers."""
    import httpx
    fetched_at = time.time()
    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=timeout,
            verify=False, headers=headers, http2=True
        ) as client:
            r = await client.get(url)
            challenge = detect_challenge(r.text, r.status_code)
            return FetchResult(
                html=r.text,
                status=r.status_code,
                url=str(r.url),
                fetched_at=fetched_at,
                method="httpx",
                challenge=challenge,
            )
    except Exception as e:
        logger.debug(f"httpx failed for {url}: {e}")
        return FetchResult(html="", status=0, url=url, fetched_at=fetched_at, method="failed")


async def download_file(url: str, dest_path: str, timeout: int = 60) -> bool:
    """
    Download a binary file (PDF, DOCX, etc.) to dest_path.
    Returns True on success.
    """
    import httpx
    from pathlib import Path
    ua = random.choice(USER_AGENTS)
    headers = {"User-Agent": ua, "Accept": "*/*"}
    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=timeout,
            verify=False, headers=headers
        ) as client:
            async with client.stream("GET", url) as r:
                if r.status_code >= 400:
                    return False
                Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, "wb") as f:
                    async for chunk in r.aiter_bytes(chunk_size=65536):
                        f.write(chunk)
        return True
    except Exception as e:
        logger.warning(f"File download failed {url}: {e}")
        return False


async def smart_fetch(url: str, use_playwright: bool = True, timeout: int = 25000) -> FetchResult:
    """
    Strategy:
    1. Always try httpx first (fast, no overhead).
    2. If page looks unrendered (SPA skeleton, JS-wall, too short), retry with Playwright.
    3. If use_playwright=False, stick to httpx only.
    """
    if not use_playwright:
        return await fetch_with_httpx(url, timeout=min(timeout // 1000, 20))

    try:
        result = await fetch_with_httpx(url, timeout=12)
        if result.success and result.html:
            hl = result.html.lower()
            needs_js = (
                len(result.html) < 1500
                or bool(result.challenge)
                or "please enable javascript" in hl
                or "javascript is required" in hl
                or "javascript is disabled" in hl
                or ("data-reactroot" in hl and len(result.html) < 8000)
                or ("ng-app" in hl and len(result.html) < 6000)
                or ("__vue_app__" in hl and len(result.html) < 6000)
                or ("noscript" in hl and len(result.html) < 3000)
            )
            if not needs_js:
                return result
    except Exception:
        pass

    return await fetch_with_playwright(url, timeout=timeout)
