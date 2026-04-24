"""
auth_fetch.py — Drop-in wrappers around smart_fetch that inject auth sessions.

Use these instead of smart_fetch when auth_required=True in the job payload.
Handles session expiry → auto re-login transparently.

KEY FIXES in this version
─────────────────────────
1. target_url is now passed to apply_session_to_playwright_context and
   build_httpx_auth_headers so those functions can skip injecting
   Authorization headers for non-https targets (prevents the Azure SDK
   'bearer token not permitted for non-TLS' error).

2. On a 401/403 the session is invalidated and a fresh login is attempted
   exactly once (MAX_RETRIES=2 means: first try, then one retry with
   force_refresh=True).

3. Fall-through to unauthenticated smart_fetch is removed from the
   session-load error path — failing loudly is better than silently
   returning wrong data on a protected portal.
"""

import asyncio
import os
from loguru import logger

from crawler.fetcher import (
    FetchResult,
    detect_challenge,
    fetch_with_httpx,
    fetch_with_playwright,
    _get_browser,
    _page_semaphore,
    STEALTH_SCRIPT,
    USER_AGENTS,
    _dismiss_popups,
)
from crawler.auth_manager import (
    auth_manager,
    apply_session_to_playwright_context,
    build_httpx_auth_headers,
)

MAX_RETRIES = 2   # first attempt + one forced re-login


async def authenticated_fetch(
    url: str,
    auth_id: str,
    redis,
    use_playwright: bool = True,
    timeout: int = 25_000,
) -> FetchResult:
    """
    Fetch a URL that requires authentication.

    Flow:
      1. Load (or create) session from Redis / Key Vault login.
      2. Make the request with session injected.
      3. If response is 401/403 → invalidate session, re-login, retry once.
    """
    for attempt in range(MAX_RETRIES):
        force_refresh = (attempt > 0)   # second attempt forces re-login

        try:
            browser = await _get_browser() if use_playwright else None
            session = await auth_manager.get_session(
                redis, auth_id, browser=browser, force_refresh=force_refresh
            )
        except Exception as e:
            logger.error(f"[auth_fetch] Could not get session for '{auth_id}': {e}")
            raise RuntimeError(
                f"Authentication failed for '{auth_id}' — cannot fetch {url}"
            ) from e

        if use_playwright:
            # Fast path: try httpx+auth first (often enough for cookie/token-based portals).
            # Fall back to Playwright only when the HTML looks JS-dependent or challenged.
            httpx_first = os.getenv("AUTH_HTTPX_FIRST", "true").lower() == "true"
            if httpx_first:
                httpx_result = await _httpx_auth_fetch(url, session)
                if httpx_result.status in (401, 403):
                    result = httpx_result
                else:
                    hl = (httpx_result.html or "").lower()
                    needs_js = (
                        (not httpx_result.success)
                        or (not httpx_result.html)
                        or bool(httpx_result.challenge)
                        or len(httpx_result.html) < 1500
                        or "please enable javascript" in hl
                        or "javascript is required" in hl
                        or "javascript is disabled" in hl
                        or ("data-reactroot" in hl and len(httpx_result.html) < 8000)
                        or ("ng-app" in hl and len(httpx_result.html) < 6000)
                        or ("__vue_app__" in hl and len(httpx_result.html) < 6000)
                        or ("noscript" in hl and len(httpx_result.html) < 3000)
                    )
                    if not needs_js:
                        return httpx_result

            result = await _playwright_auth_fetch(url, session, timeout)
        else:
            result = await _httpx_auth_fetch(url, session)

        # Re-login on auth failure
        if result.status in (401, 403):
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"[auth_fetch] Got {result.status} on {url} for '{auth_id}' "
                    f"— invalidating session and retrying"
                )
                await auth_manager.invalidate(redis, auth_id)
                continue
            else:
                logger.error(
                    f"[auth_fetch] Still {result.status} after re-login for "
                    f"'{auth_id}' on {url} — credentials may be wrong."
                )

        return result

    # Should not reach here
    logger.error(f"[auth_fetch] Exhausted retries for {url}")
    return FetchResult(html="", status=0, url=url, fetched_at=0, method="failed")


async def _playwright_auth_fetch(url: str, session: dict, timeout: int) -> FetchResult:
    """Playwright fetch with session injected into a new browser context."""
    import time
    import random
    import contextlib
    from playwright.async_api import TimeoutError as PWTimeout

    async def _do():
        async with _page_semaphore:
            fetched_at = time.time()
            ua         = random.choice(USER_AGENTS)
            context    = None
            try:
                browser = await _get_browser()
                context = await browser.new_context(
                    user_agent=ua,
                    viewport={"width": 1366, "height": 768},
                    java_script_enabled=True,
                    ignore_https_errors=True,
                )
                await context.add_init_script(STEALTH_SCRIPT)

                # FIX: pass target url so Bearer/Basic headers are skipped for http://
                await apply_session_to_playwright_context(context, session, target_url=url)

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

                page   = await context.new_page()
                status = 200
                html   = ""
                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                    if response:
                        status = response.status
                    await asyncio.sleep(0.4)
                    await _dismiss_popups(page)
                    html = await page.content()
                except PWTimeout:
                    try:
                        html   = await page.content()
                        status = 206
                    except Exception:
                        status = 408
                finally:
                    with contextlib.suppress(Exception):
                        await asyncio.shield(page.close())

                return FetchResult(
                    html=html,
                    status=status,
                    url=url,
                    fetched_at=fetched_at,
                    method="playwright+auth",
                    challenge=detect_challenge(html, status),
                )
            finally:
                if context:
                    try:
                        await asyncio.shield(context.close())
                    except Exception:
                        pass

    # Hard timeout wraps the entire operation including semaphore wait.
    # Make it slightly larger than the navigation timeout to avoid spurious cancels.
    hard_timeout_s = max(30, int(timeout / 1000) + 10)
    task = asyncio.create_task(_do())
    try:
        return await asyncio.wait_for(task, timeout=hard_timeout_s)
    except asyncio.TimeoutError:
        logger.warning(f"[auth_fetch] Playwright hard timeout for {url}")
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        return await fetch_with_httpx(url)


async def _httpx_auth_fetch(url: str, session: dict) -> FetchResult:
    """httpx fetch with auth headers — skips Authorization for non-https targets."""
    import httpx
    import time
    import random

    fetched_at = time.time()
    headers = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # FIX: pass url so bearer/basic headers are omitted for http:// targets
        **build_httpx_auth_headers(session, target_url=url),
    }
    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=15, verify=False, headers=headers
        ) as client:
            r = await client.get(url)
            return FetchResult(
                html=r.text,
                status=r.status_code,
                url=str(r.url),
                fetched_at=fetched_at,
                method="httpx+auth",
                challenge=detect_challenge(r.text, r.status_code),
            )
    except Exception as e:
        logger.warning(f"[auth_fetch] httpx failed for {url}: {e}")
        return FetchResult(html="", status=0, url=url, fetched_at=fetched_at, method="failed")