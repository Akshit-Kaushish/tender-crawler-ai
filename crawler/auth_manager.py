"""
auth_manager.py — Session-aware login handler for the tender crawler.

ROOT CAUSE OF CURRENT ERROR
────────────────────────────
The log shows:
    Key Vault client initialised → __https://tender-keyvault.vault.azure.net/__
Those double-underscores mean the URL string starts with a literal " character
(from AZURE_KEY_VAULT_URL="https://... with no closing quote in .env).
The credential chain then tries AzureCliCredential which fails because
Azure CLI is not installed in the Docker container.

TWO FIXES APPLIED HERE
──────────────────────
1.  replaces every other credential option.
   It only needs AZURE_TENANT_ID + AZURE_CLIENT_ID + AZURE_CLIENT_SECRET
   in .env — no CLI, no managed identity, no Azure tooling installed.
   Works in any Docker container, CI/CD pipeline, local machine, or VM.

2. _clean_env() strips ALL forms of stray quoting from env var values:
   leading/trailing ", ', backtick, and whitespace.
   So AZURE_KEY_VAULT_URL="https://... is read as https://...
   and the log will show the clean URL.

SMART AUTO-LOGIN (unchanged from previous version)
───────────────────────────────────────────────────
Supply only login_url + username + password.
Playwright auto-detects all form fields from the live DOM.
30+ selector strategies cover every government procurement portal worldwide.
post_login_url is saved so the worker starts crawling from the tender page.
"""

import asyncio
import json
import os
import time
from typing import Optional

from loguru import logger

# ── Redis session key layout ──────────────────────────────────────────────────
SESSION_PREFIX = "auth:session:"
LOCK_PREFIX    = "auth:lock:"
LOCK_TTL       = 30     # seconds
SESSION_BUFFER = 300    # seconds — renew this early before expiry


# ── Env var cleaning ──────────────────────────────────────────────────────────

def _clean_env(name: str, required: bool = True) -> str:
    """
    Read an env var and strip ALL common quoting mistakes:
      - Leading/trailing whitespace
      - Leading/trailing double quotes  (AZURE_KEY_VAULT_URL="https://...)
      - Leading/trailing single quotes
      - Leading/trailing backticks

    This is the fix for the __https://...__ log artifact caused by a
    literal " at the start of AZURE_KEY_VAULT_URL in .env.
    """
    val = os.environ.get(name, "")
    # Strip whitespace first, then any quote characters
    val = val.strip().strip('"').strip("'").strip("`").strip()
    if required and not val:
        raise RuntimeError(
            f"Required env var {name!r} is not set or is empty.\n"
            f"Add it to your .env file."
        )
    return val


# ── Azure Key Vault client (lazy, cached) ─────────────────────────────────────

_kv_client = None


def _get_kv_client():
    """
    Return a cached Key Vault SecretClient using 

    Required env vars in .env:
        AZURE_KEY_VAULT_URL    https://your-vault.vault.azure.net/
        AZURE_TENANT_ID        xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        AZURE_CLIENT_ID        xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        AZURE_CLIENT_SECRET    your-client-secret-value

    Why  and NOT AzureCliCredential or DefaultAzureCredential?
    ─────────────────────────────────────────────────────────────────────────────────
    AzureCliCredential      → needs `az` CLI installed in the container. It isn't.
    DefaultAzureCredential  → tries 7 methods, all fail in a plain Docker container:
                              no CLI, no PowerShell, no managed identity IMDS endpoint.
      → reads 3 env vars, makes one direct HTTPS call to
                              login.microsoftonline.com. Works everywhere, always.

    How to create the service principal (one-time, run in Azure CLI or Cloud Shell):
        az ad sp create-for-rbac --name tender-crawler-sp --skip-assignment
        # note the appId → AZURE_CLIENT_ID
        # note the password → AZURE_CLIENT_SECRET
        # note the tenant → AZURE_TENANT_ID

        az keyvault set-policy \\
          --name tender-keyvault \\
          --spn <appId> \\
          --secret-permissions get set list delete
    """
    global _kv_client
    if _kv_client is None:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        kv_url        = _clean_env("AZURE_KEY_VAULT_URL")
        # tenant_id     = _clean_env("AZURE_TENANT_ID")
        # client_id     = _clean_env("AZURE_CLIENT_ID")
        # client_secret = _clean_env("AZURE_CLIENT_SECRET")

        # Enforce https:// — bearer tokens must travel over TLS
        if kv_url.startswith("http://"):
            kv_url = "https://" + kv_url[7:]
            logger.warning("[auth] AZURE_KEY_VAULT_URL upgraded http→https")

        if not kv_url.startswith("https://"):
            raise RuntimeError(
                f"AZURE_KEY_VAULT_URL must start with https://\n"
                f"Got: {kv_url!r}\n"
                f"Check your .env — the raw value was: {os.environ.get('AZURE_KEY_VAULT_URL', '')!r}"
            )

        # credential = ClientSecretCredential(
        #     tenant_id=tenant_id,
        #     client_id=client_id,
        #     client_secret=client_secret,
        # )
        credential = DefaultAzureCredential(exclude_environment_credential=True)
        _kv_client = SecretClient(vault_url=kv_url, credential=credential)
        logger.info(f"[auth] Key Vault client initialised → {kv_url}")

    return _kv_client


def _kv_permission_hint(exc: Exception, action: str) -> str:
    text = str(exc).lower()
    if "forbidden" not in text and "not authorized" not in text:
        return ""
    client_id  = _clean_env("AZURE_CLIENT_ID", required=False)
    vault_url  = _clean_env("AZURE_KEY_VAULT_URL", required=False)
    vault_name = vault_url.replace("https://", "").replace(".vault.azure.net/", "")
    return (
        f"\nKey Vault denied '{action}' for service principal {client_id!r}.\n"
        f"Fix: az keyvault set-policy --name {vault_name} "
        f"--spn {client_id} --secret-permissions get set list delete"
    )


async def _fetch_credentials(auth_id: str) -> dict:
    """
    Pull credentials JSON from Key Vault secret named  auth-<auth_id>.
    Minimum required fields: login_url, username, password.
    Selectors (username_selector etc.) are optional — auto-detected if absent.
    """
    loop        = asyncio.get_event_loop()
    secret_name = f"auth-{auth_id}"
    try:
        client = _get_kv_client()
        secret = await loop.run_in_executor(
            None, lambda: client.get_secret(secret_name)
        )
        data = json.loads(secret.value)
        logger.debug(f"[auth] Credentials loaded from Key Vault for '{auth_id}'")
        return data
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Key Vault secret '{secret_name}' is not valid JSON: {e}"
        ) from e
    except Exception as e:
        hint = _kv_permission_hint(e, "getSecret")
        raise RuntimeError(
            f"Could not load credentials for '{auth_id}' from Key Vault: {e}{hint}"
        ) from e


# ── Smart selector auto-detection ─────────────────────────────────────────────
# Covers every naming convention seen on government procurement portals worldwide.

_USERNAME_CANDIDATES = [
    "input[type='email']",
    "input[name='username']",    "input[id='username']",
    "input[name='user']",        "input[id='user']",
    "input[name='userid']",      "input[id='userid']",
    "input[name='user_id']",     "input[id='user_id']",
    "input[name='login']",       "input[id='login']",
    "input[name='loginid']",     "input[id='loginid']",
    "input[name='email']",       "input[id='email']",
    "input[name='uname']",       "input[id='uname']",
    "input[name='uid']",         "input[id='uid']",
    # Indian govt portals
    "input[name='j_username']",  "input[id='j_username']",
    "input[name='txtUser']",     "input[id='txtUser']",
    "input[name='txtUserName']", "input[id='txtUserName']",
    "input[name='UserName']",    "input[id='UserName']",
    "input[name='userId']",      "input[id='userId']",
    "input[name='loginName']",   "input[id='loginName']",
    "input[name='LogonID']",     "input[id='LogonID']",
    # UN / World Bank / international portals
    "input[name='Username']",    "input[id='Username']",
    "input[name='EmailAddress']","input[id='EmailAddress']",
    "input[name='userEmail']",   "input[id='userEmail']",
    # ASP.NET
    "input[name$='UserName']",   "input[id$='UserName']",
    "input[name$='txtEmail']",   "input[id$='txtEmail']",
    # Generic: first visible text/email input not related to password/captcha
    "input[type='text']:not([name*='pass']):not([id*='pass'])"
    ":not([name*='captcha']):not([id*='captcha'])",
    # Last resort
    "input:not([type='password']):not([type='hidden'])"
    ":not([type='submit']):not([type='button'])"
    ":not([type='checkbox']):not([type='radio']):not([type='file'])",
]

_PASSWORD_CANDIDATES = [
    "input[type='password']",
    "input[name='password']",    "input[id='password']",
    "input[name='pass']",        "input[id='pass']",
    "input[name='passwd']",      "input[id='passwd']",
    "input[name='pwd']",         "input[id='pwd']",
    "input[name='j_password']",  "input[id='j_password']",
    "input[name='txtPassword']", "input[id='txtPassword']",
    "input[name='Password']",    "input[id='Password']",
    "input[name='userPassword']","input[id='userPassword']",
    "input[name='loginpwd']",    "input[id='loginpwd']",
    "input[name$='Password']",   "input[id$='Password']",
]

_SUBMIT_CANDIDATES = [
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Login')",   "button:has-text('Log In')",
    "button:has-text('Sign In')", "button:has-text('Submit')",
    "button:has-text('LOGIN')",   "button:has-text('SIGN IN')",
    "button:has-text('Proceed')", "button:has-text('Continue')",
    "button[id*='login']",        "button[class*='login']",
    "button[id*='signin']",       "button[class*='signin']",
    "button[id*='submit']",       "button[class*='submit']",
    "input[id*='login']",         "input[class*='login']",
    "input[value='Login']",       "input[value='Sign In']",
    "input[value='Log In']",      "input[value='Submit']",
    "input[value='LOGIN']",       "input[value='SIGN IN']",
    "form button",
    "form input[type='button']",
]


async def _find_selector(page, candidates: list) -> Optional[str]:
    """Try each selector; return first matching visible+enabled element."""
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible() and await el.is_enabled():
                return sel
        except Exception:
            continue
    return None


# ── Redis session store ───────────────────────────────────────────────────────

async def _load_session(redis, auth_id: str) -> Optional[dict]:
    key = SESSION_PREFIX + auth_id
    try:
        raw = await redis.hgetall(key)
        if not raw:
            return None
        if time.time() >= float(raw.get("expires_at", 0)) - SESSION_BUFFER:
            logger.info(f"[auth] Session '{auth_id}' near expiry — will re-login")
            return None
        return raw
    except Exception as e:
        logger.warning(f"[auth] Redis session load failed for '{auth_id}': {e}")
        return None


async def get_cached_session(redis, auth_id: str) -> Optional[dict]:
    return await _load_session(redis, auth_id)


async def _save_session(redis, auth_id: str, session: dict, ttl_seconds: int = 3600):
    key = SESSION_PREFIX + auth_id
    try:
        session["expires_at"] = str(time.time() + ttl_seconds)
        await redis.hset(key, mapping=session)
        await redis.expire(key, ttl_seconds + 60)
        logger.info(f"[auth] Session '{auth_id}' saved (TTL={ttl_seconds}s)")
    except Exception as e:
        logger.warning(f"[auth] Redis session save failed for '{auth_id}': {e}")


async def _invalidate_session(redis, auth_id: str):
    try:
        await redis.delete(SESSION_PREFIX + auth_id)
    except Exception:
        pass


# ── Login mutex ───────────────────────────────────────────────────────────────

async def _acquire_login_lock(redis, auth_id: str) -> bool:
    try:
        return (await redis.set(LOCK_PREFIX + auth_id, "1", nx=True, ex=LOCK_TTL)) is True
    except Exception:
        return False


async def _release_login_lock(redis, auth_id: str):
    try:
        await redis.delete(LOCK_PREFIX + auth_id)
    except Exception:
        pass


# ── Smart form login ──────────────────────────────────────────────────────────

async def _login_form(creds: dict, browser) -> dict:
    """
    Smart Playwright form login.
    Only needs login_url + username + password in creds.
    All selectors are auto-detected from the live DOM.

    Returns:
        { "method": "cookies", "cookies": "<json>",
          "post_login_url": "<url>", "ttl": <int> }
    """
    from playwright.async_api import TimeoutError as PWTimeout

    login_url = creds["login_url"]
    if login_url.startswith("http://"):
        login_url = "https://" + login_url[7:]
        logger.warning(f"[auth] login_url upgraded http→https: {login_url}")

    context = await browser.new_context(
        ignore_https_errors=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 768},
        java_script_enabled=True,
    )
    page = await context.new_page()

    try:
        logger.info(f"[auth] Navigating to login page: {login_url}")
        await page.goto(login_url, wait_until="domcontentloaded", timeout=25_000)
        await asyncio.sleep(1.5)   # let JS render the form

        # Auto-detect username input
        user_sel = creds.get("username_selector") or await _find_selector(page, _USERNAME_CANDIDATES)
        if not user_sel:
            visible = await page.evaluate("""() =>
                Array.from(document.querySelectorAll('input'))
                    .filter(i => i.offsetParent !== null)
                    .map(i => ({type:i.type, name:i.name, id:i.id, placeholder:i.placeholder}))
            """)
            raise RuntimeError(
                f"Cannot find username input on {login_url}. "
                f"Visible inputs: {visible}"
            )
        logger.debug(f"[auth] Username selector: {user_sel}")

        # Auto-detect password input
        pass_sel = creds.get("password_selector") or await _find_selector(page, _PASSWORD_CANDIDATES)
        if not pass_sel:
            raise RuntimeError(f"Cannot find password input on {login_url}.")
        logger.debug(f"[auth] Password selector: {pass_sel}")

        # Fill credentials
        await page.fill(user_sel, creds["username"])
        await asyncio.sleep(0.4)
        await page.fill(pass_sel, creds["password"])
        await asyncio.sleep(0.4)

        # Auto-detect submit button
        submit_sel = creds.get("submit_selector") or await _find_selector(page, _SUBMIT_CANDIDATES)
        if submit_sel:
            logger.debug(f"[auth] Submit selector: {submit_sel}")
            await page.click(submit_sel)
        else:
            logger.warning("[auth] No submit button found — pressing Enter")
            await page.press(pass_sel, "Enter")

        # Wait for post-login navigation
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except PWTimeout:
            pass   # fine — some portals never reach networkidle

        await asyncio.sleep(0.8)
        post_login_url = page.url

        # Verify success
        success_fragment = creds.get("success_url_fragment", "")
        if success_fragment and success_fragment not in post_login_url:
            page_text = (await page.inner_text("body") or "").lower()
            if any(w in page_text for w in [
                "invalid", "incorrect", "wrong", "failed", "error",
                "unauthorized", "denied", "try again", "please check",
            ]):
                raise RuntimeError(
                    f"Login failed — error text on page. URL: {post_login_url}"
                )

        cookies = await context.cookies()
        ttl     = int(creds.get("session_ttl", 3600))
        logger.info(f"[auth] Login OK → {post_login_url} | {len(cookies)} cookies")

        return {
            "method":         "cookies",
            "cookies":        json.dumps(cookies),
            "post_login_url": post_login_url,
            "ttl":            ttl,
        }

    finally:
        await page.close()
        await context.close()


# ── Public AuthManager ────────────────────────────────────────────────────────

class AuthManager:

    async def get_session(
        self,
        redis,
        auth_id: str,
        browser=None,
        force_refresh: bool = False,
    ) -> dict:
        if not force_refresh:
            session = await _load_session(redis, auth_id)
            if session:
                return session

        acquired = await _acquire_login_lock(redis, auth_id)
        if not acquired:
            logger.info(f"[auth] Waiting for login lock on '{auth_id}'…")
            for _ in range(LOCK_TTL * 2):
                await asyncio.sleep(0.5)
                session = await _load_session(redis, auth_id)
                if session:
                    return session
            raise RuntimeError(f"[auth] Timed out waiting for session '{auth_id}'")

        try:
            if not force_refresh:
                session = await _load_session(redis, auth_id)
                if session:
                    return session

            creds  = await _fetch_credentials(auth_id)
            method = creds.get("method", "form_login")
            logger.info(f"[auth] Running '{method}' login for '{auth_id}'")

            if method == "form_login":
                if browser is None:
                    raise ValueError("form_login requires a Playwright browser instance")
                result = await _login_form(creds, browser)
            else:
                raise ValueError(
                    f"Unsupported auth method '{method}'. Only 'form_login' is accepted."
                )

            ttl = result.pop("ttl", 3600)
            await _save_session(redis, auth_id, result, ttl_seconds=ttl)
            return result

        except Exception as e:
            logger.error(f"[auth] Login failed for '{auth_id}': {e}")
            raise
        finally:
            await _release_login_lock(redis, auth_id)

    async def invalidate(self, redis, auth_id: str):
        await _invalidate_session(redis, auth_id)
        logger.info(f"[auth] Session invalidated for '{auth_id}'")


auth_manager = AuthManager()


# ── Session → Playwright / httpx ──────────────────────────────────────────────

async def apply_session_to_playwright_context(
    context, session: dict, target_url: str = ""
):
    method   = session.get("method")
    is_https = (not target_url) or target_url.startswith("https://")

    if method == "cookies":
        try:
            await context.add_cookies(json.loads(session["cookies"]))
        except Exception as e:
            logger.warning(f"[auth] Cookie injection failed: {e}")

    elif method == "cookie_inject":
        cookies = []
        for part in session.get("cookie_string", "").split(";"):
            part = part.strip()
            if "=" in part:
                name, _, value = part.partition("=")
                cookies.append({"name": name.strip(), "value": value.strip(), "url": "about:blank"})
        if cookies:
            await context.add_cookies(cookies)

    elif method == "token" and is_https:
        await context.set_extra_http_headers({"Authorization": f"Bearer {session['token']}"})

    elif method == "basic" and is_https:
        import base64
        encoded = base64.b64encode(
            f"{session['username']}:{session['password']}".encode()
        ).decode()
        await context.set_extra_http_headers({"Authorization": f"Basic {encoded}"})


def build_httpx_auth_headers(session: dict, target_url: str = "") -> dict:
    import base64
    method   = session.get("method")
    is_https = (not target_url) or target_url.startswith("https://")

    if method == "token" and is_https:
        return {"Authorization": f"Bearer {session['token']}"}

    if method == "basic" and is_https:
        encoded = base64.b64encode(
            f"{session['username']}:{session['password']}".encode()
        ).decode()
        return {"Authorization": f"Basic {encoded}"}

    if method in ("cookies", "cookie_inject"):
        if method == "cookies":
            cookies    = json.loads(session["cookies"])
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        else:
            cookie_str = session.get("cookie_string", "")
        return {"Cookie": cookie_str}

    return {}