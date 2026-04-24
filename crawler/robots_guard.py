import asyncio
import time
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import httpx
from loguru import logger


ROBOTS_USER_AGENT = "TenderCrawlerAI"
ROBOTS_REFRESH_SECONDS = 3600


@dataclass
class RobotsPolicy:
    fetched_at: float
    parser: RobotFileParser
    crawl_delay: float | None = None


_robots_cache: dict[str, RobotsPolicy] = {}
_robots_lock = asyncio.Lock()


def _robots_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    base = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    return base, f"{base.rstrip('/')}/robots.txt"


async def _download_robots(robots_url: str) -> str:
    async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False) as client:
        response = await client.get(
            robots_url,
            headers={"User-Agent": ROBOTS_USER_AGENT, "Accept": "text/plain,*/*;q=0.8"},
        )
        if response.status_code >= 400:
            return ""
        return response.text


async def get_policy(url: str, user_agent: str = ROBOTS_USER_AGENT) -> RobotsPolicy:
    base, robots_url = _robots_url(url)
    cached = _robots_cache.get(base)
    if cached and (time.time() - cached.fetched_at) < ROBOTS_REFRESH_SECONDS:
        return cached

    async with _robots_lock:
        cached = _robots_cache.get(base)
        if cached and (time.time() - cached.fetched_at) < ROBOTS_REFRESH_SECONDS:
            return cached

        parser = RobotFileParser()
        parser.set_url(robots_url)
        crawl_delay = None

        try:
            body = await _download_robots(robots_url)
            lines = body.splitlines() if body else []
            parser.parse(lines)
            crawl_delay = parser.crawl_delay(user_agent) or parser.crawl_delay("*")
        except Exception as e:
            logger.warning(f"[robots] Could not fetch {robots_url}: {e}")
            parser.parse([])

        policy = RobotsPolicy(
            fetched_at=time.time(),
            parser=parser,
            crawl_delay=float(crawl_delay) if crawl_delay else None,
        )
        _robots_cache[base] = policy
        return policy


async def is_allowed(url: str, user_agent: str = ROBOTS_USER_AGENT) -> bool:
    policy = await get_policy(url, user_agent=user_agent)
    return policy.parser.can_fetch(user_agent, url)
