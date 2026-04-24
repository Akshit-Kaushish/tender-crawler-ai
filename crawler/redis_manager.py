"""
Redis queue and state management.

SESSION ISOLATION ARCHITECTURE
────────────────────────────────
Two classes of Redis keys:

  SHARED keys  — read/written by BOTH the API and worker containers.
                 Always bare names (no prefix).
                 Keys: queue:pending, queue:processing  (ONLY THESE TWO)

  SESSION keys — per-user data.  Prefixed with "s:<session_id>:" so
                 two browser tabs / API clients never see each other's
                 data.
                 Keys: logs, results, stats, session, start_urls,
                       fail_reasons, visited, failed, enqueued,
                       crawl:ctrl, workers

AZURE REDIS CACHE — MAX CONCURRENT SESSIONS
────────────────────────────────────────────
  Tier         | Max connections | Safe concurrent sessions*
  ─────────────┼─────────────────┼──────────────────────────
  Basic C0     |     256         |   ~2
  Basic C1     |    1 000        |   ~8
  Standard C1  |    1 000        |   ~8
  Standard C2  |    2 000        |  ~16
  Premium P1   |    7 500        |  ~62
  Premium P2   |  15 000        | ~125

  * Formula: floor(tier_limit / (REDIS_MAX_CONNECTIONS * total_processes))
    Default: REDIS_MAX_CONNECTIONS=20, total_processes = 1 API + N workers
    Example (Standard C1, 1 API + 5 worker containers):
      floor(1000 / (20 * 6)) = 8 concurrent sessions

  Set REDIS_MAX_CONNECTIONS env var to tune the per-process pool size.
  Raise to 30–50 on Premium tiers for higher throughput per session.

PERFORMANCE CHANGES vs v2
──────────────────────────
• enqueue()       — 4 round-trips → 1 pipeline (dedup + write batched)
• mark_visited()  — 3 round-trips → 1 pipeline
• mark_failed()   — 5 round-trips → 1 pipeline
• get_stats()     — 6+ round-trips → 1 pipeline
• increment_stat()— now also refreshes session TTL in same pipeline
• Session keys get a TTL (SESSION_TTL_SECONDS, default 2h).
  Orphaned sessions auto-clean.  TTL is reset on every stat write
  so active sessions never expire mid-run.
• queue_pending in get_stats() is SESSION-SCOPED (not global).
  This fixes the dashboard "Pending queue always 0" bug: the old
  code returned the total queue size (all sessions) which would
  show non-zero from OTHER sessions even when YOUR crawl was done.
"""
import json
import os
import time
from typing import Optional
import redis.asyncio as aioredis
from loguru import logger

# ── SHARED keys (bare — only the task queue) ──────────────────────────────────
QUEUE_PENDING     = "tender:queue:pending"
QUEUE_PROCESSING  = "tender:queue:processing"

# ── Per-session key names (always accessed via _sk()) ─────────────────────────
KEY_STATS         = "tender:stats"
KEY_SESSION       = "tender:session"
STREAM_LOGS       = "tender:logs"
LIST_RESULTS      = "tender:results"
HASH_START_URLS   = "tender:start_urls"
HASH_FAIL_REASONS = "tender:fail_reasons"
SET_VISITED       = "tender:visited"
SET_FAILED        = "tender:failed"
SET_ENQUEUED      = "tender:enqueued"
HASH_WORKERS      = "tender:workers"
KEY_CRAWL_CTRL    = "tender:crawl:ctrl"

MAX_QUEUE_SIZE        = int(os.getenv("MAX_QUEUE_SIZE", "5000"))
SESSION_TTL_SECONDS   = int(os.getenv("SESSION_TTL_SECONDS", "7200"))   # 2 h
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))


class RedisManager:
    def __init__(self, url: str = "redis://localhost:6379", session_prefix: str = ""):
        self.url = url
        self.session_prefix = session_prefix
        self.key_prefix = session_prefix   # backward-compat alias
        self._client: Optional[aioredis.Redis] = None

    def _k(self, shared_key: str) -> str:
        """Bare shared key — for queue ops only."""
        return shared_key

    def _sk(self, session_key: str) -> str:
        """Session-scoped key — prefixed with s:<id>: for user sessions."""
        return f"{self.session_prefix}{session_key}"

    async def client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(
                self.url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=REDIS_MAX_CONNECTIONS,
                socket_keepalive=True,
                socket_connect_timeout=10,
                retry_on_timeout=True,
                health_check_interval=30,
            )
        return self._client

    async def async_client(self) -> aioredis.Redis:
        return await self.client()

    async def close(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    # ─────────────────────────────────────────────────────────────────────────
    # Queue ops — SHARED bare keys (task queue)
    # Dedup uses SESSION-scoped sets so sessions don't block each other.
    # ─────────────────────────────────────────────────────────────────────────

    async def enqueue(self, url: str, depth: int, parent: str,
                      start_url: str, priority: int = 0,
                      auth_required: bool = False, auth_id: str = "",
                      session_id: str = "") -> bool:
        r = await self.client()
        try:
            # ── Batch dedup reads into one pipeline ───────────────────────
            read_pipe = r.pipeline(transaction=False)
            read_pipe.sismember(self._sk(SET_VISITED), url)
            read_pipe.sismember(self._sk(SET_FAILED), url)
            read_pipe.sismember(self._sk(SET_ENQUEUED), url)
            read_pipe.zcard(QUEUE_PENDING)
            v, f, e, qsize = await read_pipe.execute()

            if v or f or e:
                return False
            if qsize >= MAX_QUEUE_SIZE:
                logger.debug(f"Queue full ({qsize}/{MAX_QUEUE_SIZE}), dropping {url}")
                return False

            task = json.dumps({
                "url":           url,
                "depth":         depth,
                "parent":        parent,
                "start_url":     start_url,
                "enqueued_at":   time.time(),
                "auth_required": auth_required,
                "auth_id":       auth_id,
                "session_id":    session_id,
            })

            # ── Batch write ops in one pipeline ───────────────────────────
            write_pipe = r.pipeline(transaction=False)
            write_pipe.zadd(QUEUE_PENDING, {task: priority})
            write_pipe.sadd(self._sk(SET_ENQUEUED), url)
            write_pipe.hincrby(self._sk(KEY_STATS), "total_queued", 1)
            write_pipe.expire(self._sk(KEY_STATS), SESSION_TTL_SECONDS)
            results = await write_pipe.execute()
            return bool(results[0])   # zadd returns number of elements added
        except Exception as e:
            logger.error(f"enqueue failed for {url}: {e}")
            return False

    async def dequeue(self, worker_id: str) -> Optional[dict]:
        r = await self.client()
        try:
            items = await r.zpopmin(QUEUE_PENDING, 1)
            if not items:
                return None
            task_str, _score = items[0]
            task = json.loads(task_str)
            task["_worker"]  = worker_id
            task["_started"] = time.time()
            await r.hset(QUEUE_PROCESSING, task["url"], json.dumps(task))
            return task
        except Exception as e:
            logger.error(f"dequeue failed: {e}")
            return None

    async def mark_visited(self, url: str):
        """Mark done in THIS session's visited set (batched pipeline)."""
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.sadd(self._sk(SET_VISITED), url)
            pipe.hdel(QUEUE_PROCESSING, url)
            pipe.srem(self._sk(SET_ENQUEUED), url)
            await pipe.execute()
        except Exception as e:
            logger.error(f"mark_visited failed: {e}")

    async def mark_failed(self, url: str, reason: str):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.sadd(self._sk(SET_FAILED), url)
            pipe.hdel(QUEUE_PROCESSING, url)
            pipe.srem(self._sk(SET_ENQUEUED), url)
            pipe.hset(self._sk(HASH_FAIL_REASONS), url, reason[:500])
            pipe.hincrby(self._sk(KEY_STATS), "total_failed", 1)
            await pipe.execute()
        except Exception as e:
            logger.error(f"mark_failed error: {e}")

    async def is_visited(self, url: str) -> bool:
        r = await self.client()
        try:
            return bool(await r.sismember(self._sk(SET_VISITED), url))
        except Exception:
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Session-scoped pending queue count (fixes dashboard "always 0" bug)
    # ─────────────────────────────────────────────────────────────────────────

    async def get_session_queue_size(self) -> int:
        """
        Count pending tasks belonging to THIS session only.
        Returns the global count when no session prefix is set (admin view).
        Samples up to 2000 tasks to avoid blocking Redis on large queues.

        IMPORTANT: do NOT use zrange(0..N) for sampling here because the queue
        is a priority zset. If other sessions have higher-priority tasks, a
        zrange sample can miss this session entirely and incorrectly show 0.
        """
        if not self.session_prefix:
            return await self.get_queue_size()
        raw_session_id = self.session_prefix.rstrip(":")[2:]  # strip "s:"
        r = await self.client()
        try:
            cursor = 0
            count = 0
            scanned = 0
            # zscan iterates without priority bias (unlike zrange), giving
            # a more representative sample under multi-session load.
            while True:
                cursor, items = await r.zscan(QUEUE_PENDING, cursor=cursor, count=500)
                for task_str, _score in items:
                    scanned += 1
                    try:
                        if json.loads(task_str).get("session_id") == raw_session_id:
                            count += 1
                    except Exception:
                        pass
                    if scanned >= 2000:
                        return count
                if cursor == 0:
                    break
            return count
        except Exception:
            return 0

    async def get_session_processing_size(self) -> int:
        """
        Count processing tasks belonging to THIS session only.
        Returns the global count when no session prefix is set (admin view).
        Samples up to 2000 tasks to avoid blocking Redis on large hashes.
        """
        if not self.session_prefix:
            return await self.get_processing_size()
        raw_session_id = self.session_prefix.rstrip(":")[2:]  # strip "s:"
        r = await self.client()
        try:
            # Processing hash is keyed by URL; values are JSON task dicts.
            # Sample to keep the call bounded even when the hash is large.
            cursor = 0
            count = 0
            scanned = 0
            while True:
                cursor, batch = await r.hscan(QUEUE_PROCESSING, cursor=cursor, count=200)
                for _url, task_str in (batch or {}).items():
                    scanned += 1
                    if scanned > 2000:
                        return count
                    try:
                        if json.loads(task_str).get("session_id") == raw_session_id:
                            count += 1
                    except Exception:
                        pass
                if cursor == 0:
                    break
            return count
        except Exception:
            return 0

    async def is_enqueued(self, url: str) -> bool:
        """True if the URL is currently tracked as enqueued for THIS session."""
        r = await self.client()
        try:
            return bool(await r.sismember(self._sk(SET_ENQUEUED), url))
        except Exception:
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Crawl control — SESSION-scoped
    # ─────────────────────────────────────────────────────────────────────────

    async def set_crawl_state(self, state: str):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.set(self._sk(KEY_CRAWL_CTRL), state)
            pipe.expire(self._sk(KEY_CRAWL_CTRL), SESSION_TTL_SECONDS)
            await pipe.execute()
        except Exception:
            pass

    async def get_crawl_state(self) -> str:
        r = await self.client()
        try:
            val = await r.get(self._sk(KEY_CRAWL_CTRL))
            return val or "running"
        except Exception:
            return "running"

    async def pause_session(self):
        await self.set_crawl_state("paused")
        r = await self.client()
        await r.hset(self._sk(KEY_SESSION), "status", "paused")
        logger.info("Crawl paused")

    async def resume_session(self):
        await self.set_crawl_state("running")
        r = await self.client()
        await r.hset(self._sk(KEY_SESSION), "status", "running")
        logger.info("Crawl resumed")

    async def stop_crawlers(self):
        """Signal THIS session's workers to stop — does NOT affect other sessions."""
        await self.set_crawl_state("stopped")
        logger.info(f"Stop signal sent [{self.session_prefix or 'global'}]")

    async def purge_session_tasks(self):
        """
        Remove ONLY this session's tasks from the shared queues.

        This is used by the Stop endpoint to make the crawl halt quickly:
        - Removes pending tasks from the shared zset
        - Removes in-flight tasks from the shared processing hash
        - Clears this session's enqueued set so UI no longer shows "pending"

        It intentionally does NOT delete results/logs/stats.
        """
        if not self.session_prefix:
            return

        r = await self.client()
        raw_session_id = self.session_prefix.rstrip(":")[2:]  # strip "s:" prefix

        # 1) Pending zset: scan and remove matching task blobs
        try:
            cursor = 0
            to_remove: list[str] = []
            while True:
                cursor, items = await r.zscan(QUEUE_PENDING, cursor=cursor, count=500)
                for task_str, _score in items:
                    try:
                        if json.loads(task_str).get("session_id") == raw_session_id:
                            to_remove.append(task_str)
                    except Exception:
                        pass

                    if len(to_remove) >= 200:
                        await r.zrem(QUEUE_PENDING, *to_remove)
                        to_remove.clear()

                if cursor == 0:
                    break

            if to_remove:
                await r.zrem(QUEUE_PENDING, *to_remove)
        except Exception as e:
            logger.warning(f"[purge:{raw_session_id}] Pending purge failed: {e}")

        # 2) Processing hash: scan and delete matching URLs
        try:
            cursor = 0
            while True:
                cursor, kv = await r.hscan(QUEUE_PROCESSING, cursor=cursor, count=200)
                for url, task_str in (kv or {}).items():
                    try:
                        if json.loads(task_str).get("session_id") == raw_session_id:
                            await r.hdel(QUEUE_PROCESSING, url)
                    except Exception:
                        pass
                if cursor == 0:
                    break
        except Exception as e:
            logger.warning(f"[purge:{raw_session_id}] Processing purge failed: {e}")

        # 3) Session enqueued set: clear (do not wipe other session keys)
        try:
            await r.delete(self._sk(SET_ENQUEUED))
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # Worker status — SESSION-scoped
    # ─────────────────────────────────────────────────────────────────────────

    async def update_worker_status(self, worker_id: str, status: str,
                                   processed: int, session_id: str = ""):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.hset(
                self._sk(HASH_WORKERS), worker_id,
                json.dumps({
                    "id":         worker_id,
                    "status":     status,
                    "processed":  processed,
                    "session_id": session_id,
                    "updated_at": time.time(),
                })
            )
            pipe.expire(self._sk(HASH_WORKERS), SESSION_TTL_SECONDS)
            await pipe.execute()
        except Exception as e:
            logger.warning(f"update_worker_status error: {e}")

    async def get_workers(self) -> list:
        """Return workers for THIS session only."""
        r = await self.client()
        try:
            raw = await r.hgetall(self._sk(HASH_WORKERS))
            return [json.loads(v) for v in raw.values()] if raw else []
        except Exception:
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Start-URL tracking — per-session
    # ─────────────────────────────────────────────────────────────────────────

    async def set_start_url_status(self, url: str, status: str):
        r = await self.client()
        try:
            existing = await r.hget(self._sk(HASH_START_URLS), url)
            data = json.loads(existing) if existing else {"url": url, "started_at": time.time()}
            data["status"] = status
            if status == "done":
                data["done_at"] = time.time()
            elif status == "scraping":
                data["started_at"] = time.time()
            await r.hset(self._sk(HASH_START_URLS), url, json.dumps(data))
        except Exception as e:
            logger.error(f"set_start_url_status error: {e}")

    async def get_all_start_urls(self) -> list:
        r = await self.client()
        try:
            raw = await r.hgetall(self._sk(HASH_START_URLS))
            return [json.loads(v) for v in raw.values()]
        except Exception:
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Stats — per-session (fully pipelined)
    # ─────────────────────────────────────────────────────────────────────────

    async def increment_stat(self, key: str, by: int = 1):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.hincrby(self._sk(KEY_STATS), key, by)
            pipe.expire(self._sk(KEY_STATS), SESSION_TTL_SECONDS)
            await pipe.execute()
        except Exception:
            pass

    async def get_stats(self) -> dict:
        """
        Fully-pipelined stats read.
        queue_pending = THIS SESSION's pending task count (not global).
        """
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.hgetall(self._sk(KEY_STATS))
            pipe.scard(self._sk(SET_VISITED))
            pipe.scard(self._sk(SET_FAILED))
            pipe.llen(self._sk(LIST_RESULTS))
            # Global processing hash size is misleading for session dashboards;
            # we compute a session-scoped processing count below.
            pipe.hlen(QUEUE_PROCESSING)
            pipe.get(self._sk(KEY_CRAWL_CTRL))
            res = await pipe.execute()

            stats                    = res[0] or {}
            stats["total_visited"]   = res[1]
            stats["total_failed"]    = res[2]
            stats["total_results"]   = res[3]
            stats["queue_processing"]= await self.get_session_processing_size()
            stats["crawl_state"]     = res[5] or "running"
            # Session-scoped pending count — fixes dashboard "always 0" bug
            stats["queue_pending"]   = await self.get_session_queue_size()

            return {
                k: int(v) if str(v).lstrip("-").isdigit() else v
                for k, v in stats.items()
            }
        except Exception as e:
            logger.error(f"get_stats error: {e}")
            return {}

    async def get_queue_size(self) -> int:
        """Global queue size — all sessions combined."""
        r = await self.client()
        try:
            return await r.zcard(QUEUE_PENDING)
        except Exception:
            return 0

    async def get_processing_size(self) -> int:
        r = await self.client()
        try:
            return await r.hlen(QUEUE_PROCESSING)
        except Exception:
            return 0

    # ─────────────────────────────────────────────────────────────────────────
    # Results — per-session
    # ─────────────────────────────────────────────────────────────────────────

    async def save_result(self, result: dict):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.rpush(self._sk(LIST_RESULTS), json.dumps(result, ensure_ascii=False))
            pipe.ltrim(self._sk(LIST_RESULTS), -2000, -1)
            pipe.hincrby(self._sk(KEY_STATS), "total_extracted", 1)
            pipe.expire(self._sk(LIST_RESULTS), SESSION_TTL_SECONDS)
            await pipe.execute()
        except Exception as e:
            logger.error(f"save_result error: {e}")

    async def get_results(self, start: int = 0, end: int = -1) -> list:
        r = await self.client()
        try:
            items = await r.lrange(self._sk(LIST_RESULTS), start, end)
            return [json.loads(i) for i in items]
        except Exception:
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Logs — per-session
    # ─────────────────────────────────────────────────────────────────────────

    async def push_log(self, level: str, message: str, url: str = ""):
        r = await self.client()
        try:
            entry = json.dumps({"ts": time.time(), "level": level, "msg": message, "url": url})
            await r.xadd(self._sk(STREAM_LOGS), {"data": entry}, maxlen=1000, approximate=True)
        except Exception:
            pass

    async def get_logs(self, last_id: str = "0-0", count: int = 100) -> tuple[list, str]:
        r = await self.client()
        try:
            entries = await r.xread({self._sk(STREAM_LOGS): last_id}, count=count, block=None)
            if not entries:
                return [], last_id
            logs   = []
            new_id = last_id
            for _stream_name, messages in entries:
                for msg_id, fields in messages:
                    data = json.loads(fields["data"])
                    data["id"] = msg_id
                    logs.append(data)
                    new_id = msg_id
            return logs, new_id
        except Exception:
            return [], last_id

    # ─────────────────────────────────────────────────────────────────────────
    # Session state — per-session
    # ─────────────────────────────────────────────────────────────────────────

    async def start_session(self, crawl_id: str, config: dict):
        r = await self.client()
        pipe = r.pipeline(transaction=False)
        pipe.hset(self._sk(KEY_SESSION), "id",         crawl_id)
        pipe.hset(self._sk(KEY_SESSION), "config",     json.dumps(config))
        pipe.hset(self._sk(KEY_SESSION), "started_at", str(time.time()))
        pipe.hset(self._sk(KEY_SESSION), "status",     "running")
        pipe.expire(self._sk(KEY_SESSION), SESSION_TTL_SECONDS)
        await pipe.execute()
        await self.set_crawl_state("running")

    async def get_session(self) -> dict:
        r = await self.client()
        try:
            return await r.hgetall(self._sk(KEY_SESSION))
        except Exception:
            return {}

    async def end_session(self):
        r = await self.client()
        try:
            pipe = r.pipeline(transaction=False)
            pipe.hset(self._sk(KEY_SESSION), "status",   "done")
            pipe.hset(self._sk(KEY_SESSION), "ended_at", str(time.time()))
            await pipe.execute()
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # Reset — SESSION-scoped only
    # ─────────────────────────────────────────────────────────────────────────

    async def flush_session(self):
        """
        Safe per-session reset.

        1. Delete all  s:<id>:tender:*  keys.
        2. Remove only THIS session's tasks from the shared QUEUE_PENDING zset.
        3. Remove THIS session's tasks from QUEUE_PROCESSING.

        Other sessions' data and tasks are completely untouched.
        """
        r = await self.client()
        raw_session_id = self.session_prefix.rstrip(":")[2:]  # strip "s:" prefix

        # 1. Delete all session-scoped keys
        keys_to_delete: list[str] = []
        cursor = 0
        pattern = f"{self.session_prefix}tender:*"
        while True:
            cursor, batch = await r.scan(cursor, match=pattern, count=200)
            keys_to_delete.extend(batch)
            if cursor == 0:
                break
        if keys_to_delete:
            await r.delete(*keys_to_delete)
            logger.info(f"[reset:{raw_session_id}] Deleted {len(keys_to_delete)} session keys")

        # 2. Remove this session's tasks from shared pending queue
        all_pending = await r.zrange(QUEUE_PENDING, 0, -1)
        to_remove = []
        for task_str in all_pending:
            try:
                if json.loads(task_str).get("session_id") == raw_session_id:
                    to_remove.append(task_str)
            except Exception:
                pass
        if to_remove:
            await r.zrem(QUEUE_PENDING, *to_remove)
            logger.info(f"[reset:{raw_session_id}] Removed {len(to_remove)} queued tasks")

        # 3. Remove this session's processing tasks
        processing = await r.hgetall(QUEUE_PROCESSING)
        for url, task_str in processing.items():
            try:
                if json.loads(task_str).get("session_id") == raw_session_id:
                    await r.hdel(QUEUE_PROCESSING, url)
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    # Admin flush (dev / single-tenant only)
    # ─────────────────────────────────────────────────────────────────────────

    async def flush_all(self):
        """
        ADMIN ONLY — wipes the entire Redis namespace.
        Do NOT call from user-facing reset endpoints.  Use flush_session().
        """
        r = await self.client()
        keys_to_delete = []
        for pattern in ("tender:*", "s:*:tender:*"):
            cursor = 0
            while True:
                cursor, batch = await r.scan(cursor, match=pattern, count=200)
                keys_to_delete.extend(batch)
                if cursor == 0:
                    break
        if keys_to_delete:
            await r.delete(*keys_to_delete)
            logger.info(f"Admin flush: deleted {len(keys_to_delete)} keys")
        else:
            logger.info("Admin flush: Redis was already clean")


# ── Module-level singleton (no session prefix) ────────────────────────────────
redis_manager = RedisManager(url=os.getenv("REDIS_URL", "redis://localhost:6379"))
