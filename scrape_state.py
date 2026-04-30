"""Small Redis-backed guard for duplicate scrape work.

The dashboard, Celery worker, and automator all share Redis in Railway. These
keys prevent two processes from scraping the same city/category repeatedly when
the upstream site is empty, blocked, or temporarily failing.
"""

import json
import logging
import os
import re
import time
import uuid
from functools import lru_cache
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _enabled() -> bool:
    return os.getenv("SCRAPE_STATE_ENABLED", "true").lower() not in {
        "0",
        "false",
        "no",
    }


@lru_cache(maxsize=1)
def _redis_client():
    if not _enabled():
        return None

    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None

    try:
        import redis

        return redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    except Exception as exc:
        logger.warning("Scrape state disabled: Redis client unavailable: %s", exc)
        return None


def _normalize(value: Optional[str]) -> str:
    value = (value or "all").strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-") or "all"


def scrape_job_id(city: str, category: str, source: Optional[str] = None) -> str:
    return ":".join([_normalize(city), _normalize(category), _normalize(source)])


def _key(state: str, job_id: str) -> str:
    return f"scrape:{state}:{job_id}"


def claim_scrape_job(
    city: str,
    category: str,
    source: Optional[str] = None,
) -> Tuple[bool, str, Optional[str]]:
    """Return (claimed, reason, token).

    If Redis is not configured, the job is allowed so local development keeps
    working without extra setup.
    """
    client = _redis_client()
    if client is None:
        return True, "state guard unavailable", None

    job_id = scrape_job_id(city, category, source)
    try:
        for state, reason in (
            ("done", "completed recently"),
            ("empty", "empty recently"),
            ("failed", "failed recently"),
        ):
            if client.exists(_key(state, job_id)):
                return False, reason, None

        running_ttl = _env_int("SCRAPE_RUNNING_TTL_SECONDS", 1800)
        token = str(uuid.uuid4())
        claimed = client.set(_key("running", job_id), token, nx=True, ex=running_ttl)
        if not claimed:
            return False, "already running", None

        return True, "claimed", token
    except Exception as exc:
        logger.warning("Scrape state guard unavailable for %s: %s", job_id, exc)
        return True, "state guard error", None


def finish_scrape_job(
    city: str,
    category: str,
    source: Optional[str] = None,
    token: Optional[str] = None,
    *,
    count: int = 0,
    success: bool = True,
    error: Optional[str] = None,
) -> None:
    client = _redis_client()
    if client is None:
        return

    job_id = scrape_job_id(city, category, source)
    payload = json.dumps(
        {
            "city": city,
            "category": category,
            "source": source or "all",
            "count": count,
            "success": success,
            "error": error,
            "finished_at": int(time.time()),
        }
    )

    try:
        client.delete(_key("running", job_id))

        if success and count > 0:
            ttl = _env_int("SCRAPE_DONE_TTL_SECONDS", 604800)
            client.set(_key("done", job_id), payload, ex=ttl)
        elif success:
            ttl = _env_int("SCRAPE_EMPTY_TTL_SECONDS", 21600)
            client.set(_key("empty", job_id), payload, ex=ttl)
        else:
            ttl = _env_int("SCRAPE_FAILED_TTL_SECONDS", 3600)
            client.set(_key("failed", job_id), payload, ex=ttl)
    except Exception as exc:
        logger.warning("Could not update scrape state for %s: %s", job_id, exc)
