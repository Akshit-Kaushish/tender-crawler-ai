"""
worker.py — Distributed crawler worker with full session isolation.

HOW SESSION ISOLATION WORKS IN THE WORKER
───────────────────────────────────────────
Every task in the shared queue carries a `session_id` field baked in by
the API at enqueue time.  When the worker pops a task it does this:

  shared_rm  = redis_manager   (no prefix) — for dequeue / QUEUE_PROCESSING only
  session_rm = _get_session_rm(task["session_id"])  — "s:<id>:" prefix

All results, logs, visited marks, failed marks, stats, and worker status
are written through session_rm, so they land in the correct per-user
Redis keys and are never visible to other sessions.

PERFORMANCE FIX vs v2
──────────────────────
v2 had a critical bug: if a worker dequeued a task for session B while
running as session A, it would re-add the task back to the shared queue.
Under heavy multi-session load this caused an O(n) hot-loop:
  worker_A pops task_B → requeues task_B → pops task_B again → ...

Fix: workers are now bound to their SESSION_ID at the dequeue level via
a Lua script (zpopmin-if-session-matches).  Tasks for other sessions are
never popped; they stay in the queue.  Idle counter only increments when
the ENTIRE queue is empty or contains only other sessions' tasks.

If SESSION_ID is "" (legacy / single-session mode), the worker processes
all tasks regardless of session_id — identical to v1 behaviour.
"""
import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from loguru import logger
from asyncio_throttle import Throttler

from storage.blob_client import upload_blob, read_blob
from crawler.redis_manager import RedisManager, redis_manager, QUEUE_PENDING, QUEUE_PROCESSING
from crawler.fetcher import smart_fetch
from crawler.auth_fetch import authenticated_fetch
from crawler.cleaner import clean_html, extract_links_from_html, is_document_url
from crawler.ai_extractor import extract_tenders_with_ai, is_tender_page
from crawler.robots_guard import ROBOTS_USER_AGENT, get_policy, is_allowed

# ── Per-session RedisManager cache (worker side) ──────────────────────────────
_worker_session_rm: dict[str, "RedisManager"] = {}


def _get_session_rm(session_id: str) -> "RedisManager":
    """
    Return a session-scoped RedisManager for logs/results/visited/failed/workers.
    Falls back to a "__untagged__" prefix so nothing is ever silently dropped.
    """
    if not session_id:
        session_id = "__untagged__"
    if session_id not in _worker_session_rm:
        _worker_session_rm[session_id] = RedisManager(
            url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            session_prefix=f"s:{session_id}:",
        )
    return _worker_session_rm[session_id]


OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))
MAX_DEPTH          = int(os.getenv("MAX_DEPTH", "3"))
DOMAIN_DELAY       = float(os.getenv("DOMAIN_DELAY", "0.5"))
MAX_IDLE_TICKS     = int(os.getenv("MAX_IDLE_TICKS", "3000"))
TASK_TIMEOUT       = int(os.getenv("TASK_TIMEOUT", "150"))

# SESSION_ID injected by the API when it spawns this worker container.
WORKER_SESSION_ID = os.getenv("SESSION_ID", "")

_throttlers: dict[str, Throttler] = {}

# ── Lua script: atomic "pop a task for this session" ──────────────────────────
# Returns nil if the queue is empty or has no tasks for this session.
# Falls back to plain zpopmin when SESSION_ID is "" (single-session mode).
_LUA_POP_FOR_SESSION = """
local key = KEYS[1]
local session_id = ARGV[1]
local batch_size = tonumber(ARGV[2]) or 20

-- Peek at the top batch_size candidates without removing them
local items = redis.call('ZRANGE', key, 0, batch_size - 1, 'WITHSCORES')
if #items == 0 then return nil end

for i = 1, #items, 2 do
    local task_str = items[i]
    local score    = items[i+1]
    local ok, task = pcall(cjson.decode, task_str)
    if ok and task.session_id == session_id then
        -- Found a matching task — remove it atomically and return it
        redis.call('ZREM', key, task_str)
        return {task_str, score}
    end
end
return nil
"""


# ── Multilingual tender/procurement keywords ──────────────────────────────────

_HIGH_PRIORITY_KEYWORDS = frozenset([
    # ── English ──────────────────────────────────────────────────────────────
    "tender", "tenders", "bid", "bids", "bidding",
    "rfp", "rfq", "rfi", "rft", "eoi",
    "procurement", "procure",
    "contract", "contracts", "contracting",
    "solicitation", "solicitations",
    "opportunity", "opportunities",
    "notice", "notices",
    "award", "awards", "awarded",
    "prequalification", "prequalify", "prequal",
    "expression-of-interest", "expressions-of-interest",
    "call-for-proposals", "call-for-bids",
    "invitation-to-bid", "invitation-to-tender",
    "request-for-proposal", "request-for-quotation",
    "eprocure", "etender", "etenders",
    "nit",
    "gem", "gep", "gepnic", "cppp",
    "corrigendum", "addendum", "amendment",
    "lot", "lots", "package",
    "shortlist", "shortlisting",
    "supplier", "vendor", "vendors",
    "purchase", "purchasing",
    "works", "services", "goods",
    "framework", "framework-agreement",
    "open-tender", "limited-tender", "single-tender",
    "global-tender", "international-tender",
    "empanelment", "empanel",
    "rate-contract", "rate-list",
    "auction", "auctions", "e-auction",
    "reverse-auction",
    "lease", "leasing",
    "membership", "memberships",
    "registration", "enrolment", "enrollment",
    "offer", "offers",
    # ── French ───────────────────────────────────────────────────────────────
    "appel", "offres", "appel-d-offres", "aoo",
    "marche", "marches",
    "soumission", "soumissions",
    "avis", "avis-de-marche",
    "consultation", "consultations",
    "dossier", "dce",
    "attribution",
    "candidature",
    "precalification",
    "demande-de-proposition",
    "demande-cotation",
    "lettre-invitation",
    "appel-manifestation-interet",
    # ── Spanish ──────────────────────────────────────────────────────────────
    "licitacion", "licitaciones",
    "concurso", "concursos",
    "contrato", "contratos", "contratacion",
    "compra", "compras",
    "adjudicacion", "adjudicaciones",
    "pliego", "pliegos",
    "invitacion", "invitaciones",
    "cotizacion", "cotizaciones",
    "oferta", "ofertas",
    "convocatoria", "convocatorias",
    # ── Portuguese ───────────────────────────────────────────────────────────
    "licitacao", "licitacoes",
    "pregao",
    "edital", "editais",
    "cotacao", "cotacoes",
    "proposta", "propostas",
    "aquisicao",
    # ── Italian ──────────────────────────────────────────────────────────────
    "gara", "gare", "gara-appalto",
    "appalto", "appalti",
    "bando", "bandi",
    "avviso", "avvisi",
    "offerta", "offerte",
    "acquisizione", "acquisto",
    "procedura", "procedura-aperta",
    # ── German ───────────────────────────────────────────────────────────────
    "ausschreibung", "ausschreibungen",
    "vergabe", "vergaben",
    "auftrag", "auftraege",
    "bekanntmachung",
    "angebot", "angebote",
    "aufforderung",
    "beschaffung",
    # ── Arabic (transliterated) ───────────────────────────────────────────────
    "monaqasat", "monaqasa",
    "munaqasat",
    "itlaqat",
    # ── Hindi / Devanagari transliterations ──────────────────────────────────
    "nikay", "khareed", "vigyapan",
    # ── Russian (transliterated) ──────────────────────────────────────────────
    "zakupki", "goszakupki", "konkursy",
    # ── Turkish ──────────────────────────────────────────────────────────────
    "ihale", "ihaleler",
    "satinalma", "tedarik",
    "teklif",
    # ── Persian / Farsi (transliterated) ─────────────────────────────────────
    "managhese", "monaqese",
    # ── Procurement portal path patterns ─────────────────────────────────────
    "eprocurement", "e-procurement",
    "e-tender", "e-tenders",
    "etendering", "e-tendering",
    "sourcing",
    "supplier-portal", "vendor-portal",
])


def is_tender_url(url: str) -> bool:
    ul = url.lower()
    for kw in _HIGH_PRIORITY_KEYWORDS:
        if (f"/{kw}" in ul or f"/{kw}/" in ul or
                f"?{kw}" in ul or f"={kw}" in ul or
                f"&{kw}" in ul or f"-{kw}-" in ul or
                f"_{kw}_" in ul or ul.endswith(f"/{kw}") or
                ul.endswith(f"/{kw}/")):
            return True
    return False


def compute_priority(url: str) -> int:
    return 0 if is_tender_url(url) else 5


# ── Throttling ────────────────────────────────────────────────────────────────

def get_throttler(domain: str, crawl_delay: float | None = None) -> Throttler:
    if crawl_delay and crawl_delay > 0:
        key = f"{domain}|robots:{crawl_delay}"
        if key not in _throttlers:
            _throttlers[key] = Throttler(
                rate_limit=1, period=max(crawl_delay, DOMAIN_DELAY, 0.1)
            )
        return _throttlers[key]
    if domain not in _throttlers:
        _throttlers[domain] = Throttler(rate_limit=2, period=1.0)
    return _throttlers[domain]


# ── Blob helpers (always fire-and-forget) ─────────────────────────────────────

def _upload_in_background(blob_name: str, data: bytes):
    try:
        existing = read_blob(blob_name)
        upload_blob(blob_name, existing.encode("utf-8") + data)
    except Exception as e:
        logger.warning(f"Blob write failed for {blob_name}: {e}")


async def log_failed(url: str, reason: str, session_id: str = ""):
    entry = (
        json.dumps({"url": url, "reason": reason,
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S")}) + "\n"
    ).encode("utf-8")
    blob_name = f"failed/failed_{time.strftime('%Y%m%d')}.jsonl"
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(
        loop.run_in_executor(None, _upload_in_background, blob_name, entry)
    )


async def save_tenders(tenders: list[dict], session_id: str = ""):
    new_lines = (
        "\n".join(json.dumps(t, ensure_ascii=False) for t in tenders) + "\n"
    ).encode("utf-8")
    blob_name = f"tenders/tenders_{time.strftime('%Y%m%d')}.jsonl"
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(
        loop.run_in_executor(None, _upload_in_background, blob_name, new_lines)
    )


# ── Auth helper ───────────────────────────────────────────────────────────────

async def seed_post_login_url(rm: RedisManager, auth_id: str,
                               start_url: str, task: dict):
    try:
        session_id = task.get("session_id", "")
        srm        = _get_session_rm(session_id)
        redis      = await rm.client()

        # Try session-scoped sources first, then bare key (legacy)
        raw = (await redis.hget(f"s:{session_id}:tender:sources", auth_id)
               if session_id else None)
        if not raw:
            raw = await redis.hget("tender:sources", auth_id)

        base_url = ""
        if raw:
            source   = json.loads(raw)
            base_url = source.get("base_url", "")

        # Fall back to post_login_url saved in the auth session
        if not base_url or base_url == start_url:
            session_data = await redis.hgetall(f"auth:session:{auth_id}")
            post_login   = session_data.get("post_login_url", "")
            if post_login and post_login != start_url and "/Login" not in post_login:
                base_url = post_login
                logger.info(f"[auth] Using post_login_url as base: {base_url}")

        if not base_url or base_url == start_url:
            return

        added = await rm.enqueue(
            base_url, depth=0, parent=start_url,
            start_url=start_url, priority=0,
            auth_required=True, auth_id=auth_id,
            session_id=session_id,
        )
        if added:
            await srm.push_log(
                "INFO", f"[auth] Seeded post-login tender page: {base_url}", base_url
            )
    except Exception as e:
        logger.warning(f"[auth] Could not seed post-login URL for '{auth_id}': {e}")


# ── Worker status helper ──────────────────────────────────────────────────────

async def _update_worker_status(session_rm: RedisManager, worker_id: str,
                                 status: str, processed: int):
    """Write worker status into the SESSION-scoped workers hash."""
    try:
        await session_rm.update_worker_status(
            worker_id, status, processed,
            session_id=session_rm.session_prefix.rstrip(":")[2:]
        )
    except Exception as e:
        logger.warning(f"[{worker_id}] Could not update worker status: {e}")


# ── Main URL processor ────────────────────────────────────────────────────────

async def process_url(task: dict, shared_rm: RedisManager, worker_id: str):
    """
    Process one URL from the queue.

    shared_rm — used ONLY for dequeue / QUEUE_PROCESSING ops (bare keys).
    srm       — session-scoped manager for all output (logs, results, visited…).
    """
    url        = task["url"]
    depth      = task.get("depth", 0)
    start_url  = task.get("start_url", url)
    session_id = task.get("session_id", "")
    srm        = _get_session_rm(session_id)

    # Check THIS session's ctrl key before doing any work
    state = await srm.get_crawl_state()
    if state in ("stopped", "paused"):
        # Requeue so this task isn't lost — it will be picked up when resumed
        # or cleaned up by flush_session on stop/reset.
        await shared_rm.enqueue(
            url, depth, task.get("parent", ""), start_url,
            priority=compute_priority(url),
            auth_required=task.get("auth_required", False),
            auth_id=task.get("auth_id", ""),
            session_id=session_id,
        )
        return

    await srm.push_log("INFO", f"[{worker_id}] depth={depth} {url}", url)

    from urllib.parse import urlparse
    domain = urlparse(url).netloc

    if is_document_url(url):
        await srm.push_log("INFO", f"[{worker_id}] Document skip: {url}", url)
        await srm.increment_stat("docs_found")
        await srm.mark_visited(url)
        return

    try:
        if not await is_allowed(url, ROBOTS_USER_AGENT):
            reason = "Blocked by robots.txt"
            await srm.mark_failed(url, reason)
            await log_failed(url, reason, session_id=session_id)
            await srm.push_log("WARN", f"[{worker_id}] ROBOTS BLOCK: {url}", url)
            return

        policy    = await get_policy(url, ROBOTS_USER_AGENT)
        throttler = get_throttler(domain, policy.crawl_delay)
        await srm.mark_visited(url)   # session-scoped visited

        async with throttler:
            use_pw        = os.getenv("USE_PLAYWRIGHT", "true").lower() == "true"
            auth_required = task.get("auth_required", False)
            auth_id       = task.get("auth_id", "")

            if auth_required and auth_id:
                redis        = await shared_rm.client()
                fetch_result = await authenticated_fetch(
                    url, auth_id, redis, use_playwright=use_pw
                )
                await seed_post_login_url(shared_rm, auth_id, start_url, task)
            else:
                fetch_result = await smart_fetch(url, use_playwright=use_pw)

        if not fetch_result.success or not fetch_result.html:
            reason = f"HTTP {fetch_result.status} — empty or failed"
            await srm.mark_failed(url, reason)
            await log_failed(url, reason, session_id=session_id)
            await srm.push_log("ERROR", f"[{worker_id}] FAIL {url}: {reason}", url)
            return

        if fetch_result.challenge:
            reason = f"Challenge detected: {fetch_result.challenge}"
            await srm.mark_failed(url, reason)
            await log_failed(url, reason, session_id=session_id)
            await srm.push_log("WARN", f"[{worker_id}] CHALLENGE {fetch_result.challenge}: {url}", url)
            return

        await srm.increment_stat("pages_fetched")
        await srm.push_log(
            "INFO",
            f"[{worker_id}] OK [{fetch_result.method}] {len(fetch_result.html)}b — {url}",
            url,
        )

        clean_text = clean_html(fetch_result.html, base_url=url)

        if clean_text and len(clean_text) >= 80:
            if await is_tender_page(clean_text, url):
                await srm.push_log("INFO", f"[{worker_id}] TENDER PAGE: {url}", url)
                tenders = await extract_tenders_with_ai(
                    clean_text, url, fetch_result.fetched_at
                )
                if tenders:
                    for t in tenders:
                        await srm.save_result(t)
                    await save_tenders(tenders, session_id=session_id)
                    await srm.increment_stat("tender_pages_found")
                    await srm.increment_stat("tenders_extracted", len(tenders))
                    await srm.push_log(
                        "SUCCESS",
                        f"[{worker_id}] Extracted {len(tenders)} tender(s) from {url}",
                        url,
                    )
                else:
                    await srm.push_log("WARN", f"[{worker_id}] No tenders on: {url}", url)
                    await srm.mark_failed(url, "Tender page detected but AI extracted 0 tenders")
            else:
                await srm.push_log("DEBUG", f"[{worker_id}] Not tender page: {url}", url)
        else:
            await srm.push_log("WARN", f"[{worker_id}] Too little text: {url}", url)

        # ── Enqueue discovered links ──────────────────────────────────────────
        if depth < MAX_DEPTH:
            tender_links = extract_links_from_html(fetch_result.html, base_url=url)
            new_links    = 0
            for link in tender_links:
                if await srm.is_visited(link):
                    continue
                if not await is_allowed(link, ROBOTS_USER_AGENT):
                    continue
                added = await srm.enqueue(
                    link, depth + 1, parent=url,
                    start_url=start_url,
                    priority=compute_priority(link),
                    auth_required=task.get("auth_required", False),
                    auth_id=task.get("auth_id", ""),
                    session_id=session_id,
                )
                if added:
                    new_links += 1
            if new_links:
                await srm.push_log(
                    "INFO", f"[{worker_id}] Queued {new_links} links from {url}", url
                )

    except asyncio.TimeoutError:
        reason = "Timeout"
        await srm.mark_failed(url, reason)
        await log_failed(url, reason, session_id=session_id)
        await srm.push_log("ERROR", f"[{worker_id}] TIMEOUT: {url}", url)

    except Exception as e:
        reason = str(e)[:200]
        await srm.mark_failed(url, reason)
        await log_failed(url, reason, session_id=session_id)
        await srm.push_log("ERROR", f"[{worker_id}] ERROR {url}: {reason}", url)


# ── Watchdog ──────────────────────────────────────────────────────────────────

async def _watchdog(worker_id: str):
    while True:
        try:
            await asyncio.sleep(60)
            logger.info(f"[{worker_id}] Heartbeat — alive")
        except asyncio.CancelledError:
            break


# ── CrawlerWorker ─────────────────────────────────────────────────────────────

class CrawlerWorker:
    """
    A single crawler worker process.

    SESSION-BOUND DEQUEUE
    ──────────────────────
    When SESSION_ID is set, the worker uses a Lua script to atomically
    pop only tasks for its session from the shared queue.  This eliminates
    the v2 hot-loop (pop → requeue → pop → requeue) that caused CPU and
    Redis bandwidth spikes under multi-session load.

    If SESSION_ID is "" (legacy), all tasks are processed — same as v1.
    """

    def __init__(self, worker_id: str = None, session_id: str = ""):
        self.worker_id  = worker_id or f"W-{uuid.uuid4().hex[:6].upper()}"
        self.session_id = session_id or WORKER_SESSION_ID
        self.rm         = redis_manager            # shared queue ops
        self.srm        = _get_session_rm(self.session_id)  # session output
        self.running    = False
        self.processed  = 0
        # Compiled Lua script SHA (cached after first SCRIPT LOAD)
        self._lua_sha: str | None = None

    async def _load_lua(self):
        """Register the Lua dequeue script and cache its SHA."""
        if self._lua_sha is None:
            r = await self.rm.client()
            self._lua_sha = await r.script_load(_LUA_POP_FOR_SESSION)
        return self._lua_sha

    async def _dequeue_for_session(self) -> dict | None:
        """
        Pop a task that belongs to this session.

        If SESSION_ID is "", fall back to plain dequeue (v1 behaviour).
        If Lua is unavailable, fall back to the plain dequeue path too —
        which uses the old requeue approach.
        """
        if not self.session_id:
            return await self.rm.dequeue(self.worker_id)

        r = await self.rm.client()
        try:
            sha = await self._load_lua()
            result = await r.evalsha(sha, 1, QUEUE_PENDING, self.session_id, "20")
            if result is None:
                return None
            task_str, _score = result[0], result[1]
            task = json.loads(task_str)
            task["_worker"]  = self.worker_id
            task["_started"] = time.time()
            await r.hset(QUEUE_PROCESSING, task["url"], json.dumps(task))
            return task
        except Exception as e:
            logger.warning(f"[{self.worker_id}] Lua dequeue failed ({e}), using plain dequeue")
            self._lua_sha = None   # force re-load on next call
            return await self.rm.dequeue(self.worker_id)

    async def run(self):
        self.running = True
        logger.info(
            f"Worker {self.worker_id} started "
            f"session={self.session_id or '(none)'} "
            f"concurrency={WORKER_CONCURRENCY} timeout={TASK_TIMEOUT}s"
        )
        await _update_worker_status(self.srm, self.worker_id, "running", 0)

        watchdog = asyncio.create_task(_watchdog(self.worker_id))
        tasks: list[asyncio.Task] = []
        idle = 0

        try:
            while self.running:
                # ── Check THIS session's ctrl key ──────────────────────────
                state = await self.srm.get_crawl_state()

                if state == "stopped":
                    logger.info(f"Worker {self.worker_id} received stop signal")
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    break

                if state == "paused":
                    await asyncio.sleep(0.5)
                    continue

                tasks = [t for t in tasks if not t.done()]

                if len(tasks) >= WORKER_CONCURRENCY:
                    await asyncio.sleep(0.05)
                    continue

                # ── Session-bound dequeue (no requeue hot-loop) ────────────
                task_data = await self._dequeue_for_session()

                if task_data is None:
                    idle += 1
                    if idle > MAX_IDLE_TICKS:
                        pending    = await self.rm.get_queue_size()
                        processing = await self.rm.get_processing_size()
                        if pending == 0 and processing == 0 and not tasks:
                            logger.info(f"Worker {self.worker_id} done — queue empty")
                            break
                        else:
                            logger.info(
                                f"[{self.worker_id}] Idle — pending={pending} "
                                f"processing={processing} tasks={len(tasks)}"
                            )
                            idle = 0
                    await asyncio.sleep(0.1)
                    continue

                idle = 0
                self.processed += 1

                async def bounded(t, wid=self.worker_id):
                    try:
                        await asyncio.wait_for(
                            process_url(t, self.rm, wid),
                            timeout=TASK_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        url = t.get("url", "unknown")
                        sid = t.get("session_id", "")
                        srm = _get_session_rm(sid)
                        logger.error(f"[{wid}] HARD TIMEOUT ({TASK_TIMEOUT}s): {url}")
                        try:
                            await srm.mark_failed(url, f"Hard timeout {TASK_TIMEOUT}s")
                            await srm.push_log("ERROR", f"[{wid}] HARD TIMEOUT: {url}", url)
                        except Exception:
                            pass
                    except Exception as exc:
                        logger.error(
                            f"[{wid}] Unhandled: {task_data.get('url')}: {exc}",
                            exc_info=True,
                        )

                tasks.append(asyncio.create_task(bounded(task_data)))

                if self.processed % 10 == 0:
                    await _update_worker_status(
                        self.srm, self.worker_id, "running", self.processed
                    )

        finally:
            watchdog.cancel()
            try:
                await watchdog
            except asyncio.CancelledError:
                pass

        still_running = [t for t in tasks if not t.done()]
        if still_running:
            logger.info(f"Worker {self.worker_id} draining {len(still_running)} tasks…")
            await asyncio.gather(*still_running, return_exceptions=True)

        await _update_worker_status(self.srm, self.worker_id, "done", self.processed)
        await self.srm.push_log(
            "INFO",
            f"Worker {self.worker_id} finished. Processed {self.processed} URLs."
        )
        logger.info(f"Worker {self.worker_id} finished. Processed {self.processed} URLs.")

    def stop(self):
        self.running = False


async def main():
    worker = CrawlerWorker(session_id=WORKER_SESSION_ID)
    try:
        await worker.run()
    finally:
        await redis_manager.close()


if __name__ == "__main__":
    asyncio.run(main())