"""
FastAPI — TenderCrawler AI  (v4 — complete session isolation)

SESSION ISOLATION — ALL LAYERS
────────────────────────────────
  SHARED Redis keys:   queue:pending, queue:processing
  SESSION Redis keys:  everything else — prefixed "s:<session_id>:"
                       (logs, results, stats, session, visited, failed,
                        enqueued, workers, crawl:ctrl, start_urls, fail_reasons)

WORKER SPAWNING PER SESSION
────────────────────────────
  When /api/crawl is called, the API spawns exactly N worker containers
  (N = req.max_workers, default = WORKER_CONCURRENCY env, fallback 5).
  Each container gets SESSION_ID as an env variable so it only processes
  tasks for that session and writes to that session's Redis keys.

  Workers are spawned via `docker run` subprocess.  The API tracks running
  containers per session in _session_containers so it can kill them on
  stop/reset.

  If Docker is unavailable (e.g. dev mode), workers fall back to
  in-process asyncio tasks (same CrawlerWorker class).

ENDPOINT ISOLATION GUARANTEES
──────────────────────────────
  /api/crawl    → seeds URLs + spawns N session workers
  /api/stop     → signals s:<id>:crawl:ctrl = "stopped" + kills containers
  /api/pause    → signals s:<id>:crawl:ctrl = "paused"
  /api/resume   → signals s:<id>:crawl:ctrl = "running"
  /api/reset    → flush_session() (only this session's keys + tasks)
  /api/status   → workers filtered to this session
  /api/workers  → workers filtered to this session
  /api/logs/stream → s:<id>:tender:logs stream
  /api/results  → s:<id>:tender:results list
  /api/download → s:<id>: results / fail_reasons
"""
import asyncio
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from pydantic import BaseModel, field_validator
from loguru import logger

from storage.blob_client import upload_blob, delete_all_blobs
from crawler.auth_manager import get_cached_session
from crawler.redis_manager import RedisManager
from crawler.source_registry import (
    get_source,
    list_sources,
    match_source_for_url,
    normalize_url,
    register_source,
)

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REDIS_URL          = os.getenv("REDIS_URL", "redis://localhost:6379")
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))

# Docker image name used to spawn worker containers.
# Must match the image built from Dockerfile.workerrr.
WORKER_IMAGE = os.getenv("WORKER_IMAGE", "tender_crawler_auth_v2-worker")

app = FastAPI(title="TenderCrawler AI", version="4.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ── Per-session state ─────────────────────────────────────────────────────────
_rm_cache:             dict[str, RedisManager]         = {}
_monitor_tasks:        dict[str, asyncio.Task]         = {}
_session_containers:   dict[str, list[str]]            = {}   # session_id → [container_id, ...]
_inprocess_workers:    dict[str, list[asyncio.Task]]   = {}   # fallback: in-process workers


def get_rm(session_id: str) -> RedisManager:
    """Return (or create) a session-scoped RedisManager."""
    if session_id not in _rm_cache:
        _rm_cache[session_id] = RedisManager(
            url=REDIS_URL,
            session_prefix=f"s:{session_id}:",
        )
    return _rm_cache[session_id]


def _resolve_session(
    x_session_id: Optional[str] = None,
    session_id_param: Optional[str] = None,
) -> str:
    sid = (x_session_id or session_id_param or "default").strip()
    return sid if sid else "default"


# ── Startup check ─────────────────────────────────────────────────────────────

@app.on_event("startup")
async def _startup_checks():
    issues = []
    kv_url = os.environ.get("AZURE_KEY_VAULT_URL", "").strip().strip('"').strip("'")
    if not kv_url:
        issues.append("AZURE_KEY_VAULT_URL is not set")
    elif not kv_url.startswith("https://"):
        issues.append(f"AZURE_KEY_VAULT_URL must start with https://, got: {kv_url!r}")
    for var in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):
        if not os.environ.get(var, "").strip().strip('"').strip("'"):
            issues.append(f"{var} is not set")
    if issues:
        for issue in issues:
            logger.warning(f"[startup] ⚠ {issue}")
    else:
        logger.info(f"[startup] Azure KV config looks good → {kv_url}")


# ── Request models ─────────────────────────────────────────────────────────────

class CrawlRequest(BaseModel):
    urls:           list[str]
    max_depth:      int   = 3
    max_workers:    int   = WORKER_CONCURRENCY   # per-session worker count
    domain_delay:   float = 0.5
    use_playwright: bool  = True
    session_name:   str   = ""
    auth_required:  bool  = False
    auth_id:        str   = ""


class SourceCreateRequest(BaseModel):
    name:                  str
    base_url:              str
    login_url:             str   = ""
    username:              str
    password:              str
    username_selector:     str   = "#username"
    password_selector:     str   = "#password"
    submit_selector:       str   = "button[type=submit]"
    success_url_fragment:  str   = ""
    session_ttl:           int   = 3600
    notes:                 str   = ""

    @field_validator("base_url", "login_url", mode="before")
    @classmethod
    def _https_only(cls, v: str) -> str:
        v = (v or "").strip().strip('"').strip("'")
        if v and v.startswith("http://"):
            v = "https://" + v[7:]
        return v

    @field_validator("username", mode="before")
    @classmethod
    def _username_nonempty(cls, v: str) -> str:
        if not (v or "").strip():
            raise ValueError("username is required")
        return v.strip()

    @field_validator("password", mode="before")
    @classmethod
    def _password_nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("password is required")
        return v


# ── Worker spawning ───────────────────────────────────────────────────────────

def _docker_available() -> bool:
    """Check if docker CLI is accessible."""
    try:
        r = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=5
        )
        return r.returncode == 0
    except Exception:
        return False


def _spawn_docker_worker(session_id: str, env_overrides: dict) -> Optional[str]:
    """
    Spawn a single worker Docker container for the given session.
    Returns the container ID, or None on failure.
    """
    # Build env flags: pass through the current process env + overrides
    env_args = []
    passthrough = [
        "REDIS_URL", "OPENAI_API_KEY", "AZURE_KEY_VAULT_URL",
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
        "AZURE_STORAGE_CONNECTION_STRING", "OUTPUT_DIR",
        "USE_PLAYWRIGHT", "MAX_DEPTH", "DOMAIN_DELAY",
        "WORKER_CONCURRENCY", "MAX_IDLE_TICKS", "TASK_TIMEOUT",
        "PLAYWRIGHT_MAX_PAGES", "AUTH_HTTPX_FIRST",
    ]
    for key in passthrough:
        val = os.environ.get(key, "")
        if val:
            env_args += ["-e", f"{key}={val}"]
    # Session-specific overrides
    for k, v in env_overrides.items():
        env_args += ["-e", f"{k}={v}"]

    cmd = [
        "docker", "run", "-d", "--rm",
        "--network", "tender_crawler_auth_v2_default",  # same compose network
        *env_args,
        WORKER_IMAGE,
        "python", "-m", "crawler.worker",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            cid = result.stdout.strip()
            logger.info(f"[spawn] Container {cid[:12]} started for session={session_id}")
            return cid
        else:
            logger.error(f"[spawn] docker run failed: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"[spawn] docker run exception: {e}")
        return None


def _kill_session_containers(session_id: str):
    """Stop all Docker containers spawned for a session."""
    cids = _session_containers.pop(session_id, [])
    for cid in cids:
        try:
            subprocess.run(
                ["docker", "stop", cid],
                capture_output=True, timeout=15
            )
            logger.info(f"[kill] Stopped container {cid[:12]} for session={session_id}")
        except Exception as e:
            logger.warning(f"[kill] Could not stop container {cid[:12]}: {e}")


async def _spawn_inprocess_workers(session_id: str, n: int):
    """
    Fallback: run workers as in-process asyncio tasks when Docker
    is unavailable (e.g. local dev without Docker socket).
    """
    from crawler.worker import CrawlerWorker
    tasks = []
    for i in range(n):
        worker_id = f"W-{session_id[:6].upper()}-{i+1}"
        w = CrawlerWorker(worker_id=worker_id, session_id=session_id)
        t = asyncio.create_task(w.run())
        tasks.append(t)
        logger.info(f"[spawn-inproc] Worker {worker_id} started for session={session_id}")
    _inprocess_workers[session_id] = tasks


def _kill_inprocess_workers(session_id: str):
    tasks = _inprocess_workers.pop(session_id, [])
    for t in tasks:
        if not t.done():
            t.cancel()


async def spawn_session_workers(session_id: str, n: int, config: dict):
    """
    Spawn exactly N workers for a session.
    Tries Docker first; falls back to in-process coroutines.
    """
    use_docker = _docker_available()
    env_overrides = {
        "SESSION_ID":          session_id,
        # Per-process concurrency is controlled by WORKER_CONCURRENCY env var.
        # `n` controls how many worker processes/containers we spawn for the session.
        "WORKER_CONCURRENCY":  str(os.getenv("WORKER_CONCURRENCY", str(WORKER_CONCURRENCY))),
        "MAX_DEPTH":           str(config.get("max_depth", 3)),
        "DOMAIN_DELAY":        str(config.get("domain_delay", 0.5)),
        "USE_PLAYWRIGHT":      str(config.get("use_playwright", True)).lower(),
    }

    if use_docker:
        cids = []
        for i in range(n):
            cid = _spawn_docker_worker(session_id, env_overrides)
            if cid:
                cids.append(cid)
        _session_containers[session_id] = cids
        logger.info(f"[spawn] {len(cids)}/{n} Docker workers started for session={session_id}")
    else:
        logger.warning("[spawn] Docker unavailable — using in-process workers")
        await _spawn_inprocess_workers(session_id, n)


# ── Monitor helpers ───────────────────────────────────────────────────────────

def _cancel_monitor(session_id: str):
    task = _monitor_tasks.get(session_id)
    if task and not task.done():
        task.cancel()
    _monitor_tasks.pop(session_id, None)


# ── Crawl seeding ─────────────────────────────────────────────────────────────

async def seed_start_urls(
    session_id: str,
    urls: list[str],
    config: dict,
    auth_required: bool = False,
    auth_id: str = "",
):
    rm = get_rm(session_id)
    crawl_sid = str(uuid.uuid4())[:8].upper()
    await rm.start_session(crawl_sid, config)

    await rm.push_log(
        "INFO",
        f"╔══ Session started ══╗  dashboard_session={session_id}  crawl_id={crawl_sid}",
    )
    await rm.push_log("INFO", f"Seeding {len(urls)} URL(s) into worker queue")

    seeded = 0
    redis  = await rm.client()

    async def _seed(url):
        nonlocal seeded
        source = await match_source_for_url(redis, url)
        resolved_auth_required = auth_required
        resolved_auth_id       = auth_id
        if source and source.get("auth_required"):
            resolved_auth_required = True
            resolved_auth_id       = source["auth_id"]
            await rm.push_log("INFO", f"Protected source matched for {url}: {source['name']}")
        await rm.set_start_url_status(url, "pending")
        added = await rm.enqueue(
            url, depth=0, parent="", start_url=url, priority=0,
            auth_required=resolved_auth_required,
            auth_id=resolved_auth_id,
            session_id=session_id,
        )
        if added:
            seeded += 1

    await asyncio.gather(*[_seed(u) for u in urls])
    await rm.push_log(
        "INFO",
        f"Queued {seeded}/{len(urls)} URL(s) — spawning {config['max_workers']} worker(s)"
    )
    await rm.increment_stat("start_urls_count", seeded)

    # Spawn workers AFTER seeding so they have tasks immediately
    await spawn_session_workers(session_id, config["max_workers"], config)

    _cancel_monitor(session_id)
    _monitor_tasks[session_id] = asyncio.create_task(_monitor(session_id, urls))


async def _monitor(session_id: str, urls: list[str]):
    rm = get_rm(session_id)
    while True:
        try:
            all_done = True
            r = await rm.client()
            for url in urls:
                if await rm.is_visited(url):
                    await rm.set_start_url_status(url, "done")
                elif await r.hexists("tender:queue:processing", url):
                    await rm.set_start_url_status(url, "scraping")
                    all_done = False
                # Pending queue is a zset of JSON task blobs, not URLs.
                # Use the per-session enqueued set to track "queued but not finished".
                elif await rm.is_enqueued(url):
                    await rm.set_start_url_status(url, "pending")
                    all_done = False

            workers          = await rm.get_workers()   # session-scoped
            all_workers_done = (
                bool(workers)
                and all(w.get("status") in ("done", "stopped") for w in workers)
            )

            if all_done and all_workers_done and (await rm.get_session_queue_size()) == 0 and (await rm.get_session_processing_size()) == 0:
                await rm.end_session()
                await rm.push_log("INFO", "All workers finished — session complete ✓")
                break

            await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Monitor error: {e}")
            await asyncio.sleep(3)


# ── API endpoints ──────────────────────────────────────────────────────────────

@app.post("/api/crawl")
async def start_crawl(
    req: CrawlRequest,
    bg: BackgroundTasks,
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    if not req.urls:
        raise HTTPException(400, "No URLs provided")
    valid  = [normalize_url(u) for u in req.urls if u.strip()]
    n_workers = max(1, req.max_workers or WORKER_CONCURRENCY)
    config = {
        "max_depth":      req.max_depth,
        "max_workers":    n_workers,
        "domain_delay":   req.domain_delay,
        "use_playwright": req.use_playwright,
        "session_name":   req.session_name or f"Session-{int(time.time())}",
        "url_count":      len(valid),
    }
    os.environ["USE_PLAYWRIGHT"] = str(req.use_playwright).lower()
    os.environ["MAX_DEPTH"]      = str(req.max_depth)
    os.environ["DOMAIN_DELAY"]   = str(req.domain_delay)

    bg.add_task(seed_start_urls, session_id, valid, config, req.auth_required, req.auth_id)
    return {
        "status":     "started",
        "session_id": session_id,
        "queued":     len(valid),
        "workers":    n_workers,
        "config":     config,
    }


@app.get("/api/sources")
async def get_sources(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    redis      = await get_rm(session_id).client()
    sources    = await list_sources(redis)
    enriched   = []
    for source in sources:
        session = await get_cached_session(redis, source["auth_id"])
        enriched.append({
            **source,
            "has_active_session": bool(session),
            "session_method":     (session or {}).get("method", ""),
        })
    return {"sources": enriched, "count": len(enriched)}


@app.post("/api/sources")
async def create_source(
    req: SourceCreateRequest,
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    redis      = await get_rm(session_id).client()
    try:
        source = await register_source(redis, {
            "name":                 req.name,
            "base_url":             req.base_url,
            "login_url":            req.login_url or req.base_url,
            "username":             req.username,
            "password":             req.password,
            "username_selector":    req.username_selector,
            "password_selector":    req.password_selector,
            "submit_selector":      req.submit_selector,
            "success_url_fragment": req.success_url_fragment,
            "session_ttl":          req.session_ttl,
            "notes":                req.notes,
            "method":               "form_login",
            "auth_required":        True,
        })
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"status": "saved", "source": source}


@app.get("/api/sources/resolve")
async def resolve_source(
    url: str = Query(...),
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    redis      = await get_rm(session_id).client()
    normalized = normalize_url(url)
    source     = await match_source_for_url(redis, normalized)
    if not source:
        return {
            "url": normalized, "matched": False,
            "auth_required": False, "has_active_session": False,
        }
    session = await get_cached_session(redis, source["auth_id"])
    return {
        "url":                normalized,
        "matched":            True,
        "source":             source,
        "auth_required":      bool(source.get("auth_required")),
        "has_active_session": bool(session),
        "session_method":     (session or {}).get("method", ""),
    }


@app.get("/api/sources/{auth_id}/session")
async def get_source_session(
    auth_id: str,
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    redis      = await get_rm(session_id).client()
    source     = await get_source(redis, auth_id)
    if not source:
        raise HTTPException(404, "Source not found")
    session = await get_cached_session(redis, auth_id)
    return {
        "source":             source,
        "has_active_session": bool(session),
        "session_method":     (session or {}).get("method", ""),
    }


@app.post("/api/pause")
async def pause_crawl(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    state      = await rm.get_crawl_state()
    if state != "running":
        raise HTTPException(400, f"Cannot pause — current state is '{state}'")
    await rm.pause_session()
    await rm.push_log("INFO", "Crawl paused by user")
    return {"status": "paused"}


@app.post("/api/resume")
async def resume_crawl(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    state      = await rm.get_crawl_state()
    if state != "paused":
        raise HTTPException(400, f"Cannot resume — current state is '{state}'")
    await rm.resume_session()
    await rm.push_log("INFO", "Crawl resumed by user")
    return {"status": "running"}


@app.post("/api/stop")
async def stop_crawl(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    _cancel_monitor(session_id)
    rm = get_rm(session_id)
    # Signal session workers via SESSION-scoped ctrl key
    await rm.stop_crawlers()
    await rm.end_session()
    await rm.push_log("INFO", "Crawl stopped by user")
    # Kill containers belonging to this session
    _kill_session_containers(session_id)
    _kill_inprocess_workers(session_id)
    return {"status": "stopped"}


@app.get("/api/status")
async def get_status(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    stats      = await rm.get_stats()
    session    = await rm.get_session()
    # Workers are SESSION-scoped
    workers          = await rm.get_workers()
    all_workers_done = (
        bool(workers)
        and all(w.get("status") in ("done", "stopped") for w in workers)
    )

    # A session is "empty" when it has no pending or processing tasks for THIS session.
    # Using global queue sizes here is misleading under multi-session load.
    session_pending    = await rm.get_session_queue_size()
    session_processing = await rm.get_session_processing_size()
    queue_empty        = (session_pending == 0 and session_processing == 0)

    crawl_complete = bool(
        all_workers_done
        and queue_empty
        and session.get("status") == "done"
    )
    return {
        "session":          session,
        "stats":            stats,
        "workers":          workers,
        "all_workers_done": all_workers_done,
        "crawl_complete":   crawl_complete,
        "crawl_state":      stats.get("crawl_state", "running"),
        "timestamp":        time.time(),
        "session_id":       session_id,
        "queue_pending_session":    session_pending,
        "queue_processing_session": session_processing,
    }


@app.get("/api/start-urls")
async def get_start_urls(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    urls       = await rm.get_all_start_urls()
    return {
        "urls":     sorted(urls, key=lambda x: x.get("status", "")),
        "total":    len(urls),
        "done":     sum(1 for u in urls if u.get("status") == "done"),
        "scraping": sum(1 for u in urls if u.get("status") == "scraping"),
        "pending":  sum(1 for u in urls if u.get("status") == "pending"),
    }


@app.get("/api/results")
async def get_results(
    page:      int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    start      = (page - 1) * page_size
    results    = await rm.get_results(start, start + page_size - 1)
    stats      = await rm.get_stats()
    total      = int(stats.get("total_results", 0))
    return {
        "tenders":   results,
        "page":      page,
        "page_size": page_size,
        "total":     total,
        "pages":     (total + page_size - 1) // page_size if total else 0,
    }


@app.get("/api/logs/stream")
async def stream_logs(
    session_id: Optional[str] = Query(default=None),
    x_session_id: Optional[str] = Header(default=None),
):
    sid = _resolve_session(x_session_id, session_id)
    rm  = get_rm(sid)

    async def gen() -> AsyncGenerator[str, None]:
        last_id = "0-0"
        while True:
            try:
                logs, last_id = await rm.get_logs(last_id=last_id, count=50)
                for log in logs:
                    yield f"data: {json.dumps(log)}\n\n"
                if not logs:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'ts': time.time()})}\n\n"
                await asyncio.sleep(0.4)
            except asyncio.CancelledError:
                break
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'msg': str(e)})}\n\n"
                await asyncio.sleep(1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/logs")
async def get_logs(
    count: int = Query(200, ge=1, le=2000),
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    logs, _    = await rm.get_logs(last_id="0-0", count=count)
    return {"logs": logs, "count": len(logs)}


@app.get("/api/failed")
async def get_failed(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    r          = await rm.client()
    fr         = await r.hgetall(rm._sk("tender:fail_reasons"))
    return {"failed": [{"url": k, "reason": v} for k, v in fr.items()], "total": len(fr)}


@app.get("/api/workers")
async def get_workers(x_session_id: Optional[str] = Header(default=None)):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)
    workers    = await rm.get_workers()   # SESSION-scoped
    return {"workers": workers, "count": len(workers)}


# ── Downloads ──────────────────────────────────────────────────────────────────

@app.get("/api/download/results")
async def download_results(
    bg: BackgroundTasks,
    session_id: Optional[str] = Query(default=None),
    x_session_id: Optional[str] = Header(default=None),
):
    sid     = _resolve_session(x_session_id, session_id)
    rm      = get_rm(sid)
    results = await rm.get_results(0, -1)
    if not results:
        raise HTTPException(404, "No results yet — run a crawl first")
    filename  = f"tenders_{time.strftime('%Y%m%d_%H%M%S')}.jsonl"
    content   = "\n".join(json.dumps(r, ensure_ascii=False) for r in results).encode("utf-8")
    blob_name = f"tenders/{sid}_export_{time.strftime('%Y%m%d_%H%M%S')}.jsonl"
    bg.add_task(_save_blob_background, blob_name, content)
    return Response(
        content=content,
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/download/failed")
async def download_failed(
    bg: BackgroundTasks,
    session_id: Optional[str] = Query(default=None),
    x_session_id: Optional[str] = Header(default=None),
):
    sid = _resolve_session(x_session_id, session_id)
    rm  = get_rm(sid)
    r   = await rm.client()
    fr  = await r.hgetall(rm._sk("tender:fail_reasons"))
    if not fr:
        raise HTTPException(404, "No failed links yet")
    filename  = f"failed_{time.strftime('%Y%m%d_%H%M%S')}.jsonl"
    content   = "\n".join(
        json.dumps({"url": k, "reason": v}) for k, v in fr.items()
    ).encode("utf-8")
    blob_name = f"failed/{sid}_export_{time.strftime('%Y%m%d_%H%M%S')}.jsonl"
    bg.add_task(_save_blob_background, blob_name, content)
    return Response(
        content=content,
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _save_blob_background(blob_name: str, data: bytes):
    try:
        upload_blob(blob_name, data)
    except Exception as e:
        logger.warning(f"Background blob save failed (download already succeeded): {e}")


# ── Reset — SESSION-scoped only ────────────────────────────────────────────────

@app.delete("/api/reset")
async def reset(
    bg: BackgroundTasks,
    x_session_id: Optional[str] = Header(default=None),
):
    session_id = _resolve_session(x_session_id)
    rm         = get_rm(session_id)

    # Stop signal → kill containers → flush session keys only
    await rm.stop_crawlers()
    _cancel_monitor(session_id)
    _kill_session_containers(session_id)
    _kill_inprocess_workers(session_id)

    await asyncio.sleep(0.5)
    await rm.flush_session()          # only this session's keys + tasks
    bg.add_task(_delete_blobs_background)
    return {"status": "reset", "message": "Session data cleared. Other sessions unaffected."}


def _delete_blobs_background():
    try:
        deleted = delete_all_blobs()
        logger.info(f"Reset: deleted {deleted} blobs from storage")
    except Exception as e:
        logger.warning(f"Blob cleanup error: {e}")


# ── Admin: full wipe ───────────────────────────────────────────────────────────

@app.delete("/api/admin/flush-all")
async def admin_flush_all():
    """
    ADMIN ONLY — wipes all Redis keys for all sessions and the shared queue.
    Not available via the dashboard; call directly with curl/Postman.
    """
    rm = RedisManager(url=REDIS_URL)
    await rm.flush_all()
    return {"status": "flushed", "message": "All Redis keys cleared."}


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    try:
        from crawler.redis_manager import RedisManager as _RM
        r = await _RM(url=REDIS_URL).client()
        await r.ping()
        return {"status": "ok", "redis": "connected", "ts": time.time()}
    except Exception as e:
        return {"status": "error", "redis": str(e)}


@app.get("/api/health/detailed")
async def health_detailed():
    """
    Extended health check — returns Redis latency, connection info,
    and a warning when latency suggests a remote (Azure) endpoint
    is degraded.  Used by the dashboard API Monitor panel.
    """
    import urllib.parse as _up
    result: dict = {"ts": time.time(), "redis_url_host": ""}
    try:
        parsed = _up.urlparse(REDIS_URL)
        result["redis_url_host"] = parsed.hostname or ""
        result["redis_tls"] = REDIS_URL.startswith("rediss://")
        result["is_external"] = not (
            (parsed.hostname or "").startswith("localhost") or
            (parsed.hostname or "").startswith("127.") or
            (parsed.hostname or "") == "redis"
        )

        from crawler.redis_manager import RedisManager as _RM
        rm = _RM(url=REDIS_URL)
        r  = await rm.client()

        # Measure round-trip latency (3 pings, take median)
        latencies = []
        for _ in range(3):
            t0 = time.perf_counter()
            await r.ping()
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        median_ms = latencies[1]

        result["redis_status"]     = "ok"
        result["latency_ms"]       = round(median_ms, 2)
        result["latency_p50_ms"]   = round(latencies[0], 2)
        result["latency_p100_ms"]  = round(latencies[2], 2)
        result["latency_warn"]     = median_ms > 15   # >15ms = likely tier pressure
        result["latency_critical"] = median_ms > 50

        # Queue snapshot
        result["queue_pending_global"]    = await r.zcard("tender:queue:pending")
        result["queue_processing_global"] = await r.hlen("tender:queue:processing")

        # Active sessions count (scan for s:*:tender:session keys)
        session_keys = []
        cursor = 0
        while True:
            cursor, batch = await r.scan(cursor, match="s:*:tender:session", count=100)
            session_keys.extend(batch)
            if cursor == 0:
                break
        result["active_sessions"] = len(session_keys)

        result["status"] = "ok"
    except Exception as e:
        result["status"]       = "error"
        result["redis_status"] = str(e)
        result["latency_ms"]   = -1

    return result


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    for p in [
        Path(__file__).parent.parent / "dashboard" / "index.html",
        Path(__file__).parent.parent / "index.html",
    ]:
        if p.exists():
            return HTMLResponse(p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>TenderCrawler</h1><a href='/docs'>API Docs</a>")
