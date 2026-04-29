"""
Compatibility wrapper for older entrypoints that still import fast_scraper.

The production scraper was moved to polite_http_scraper.py and ContactScraper,
but automate_100_cities.py and worker.py still depend on this module name.
Keeping this shim prevents Railway from restarting the dashboard because an
optional automator import failed.
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from polite_http_scraper import PoliteHTTPScraper

logger = logging.getLogger("fast_scraper")


class FastHTTPScraper(PoliteHTTPScraper):
    """Backwards-compatible alias for the current polite HTTP scraper."""


@dataclass
class FastScraperConfig:
    raw: Dict[str, Any]

    @property
    def max_concurrent(self) -> int:
        settings = self.raw.get("scraper_settings", {}) if isinstance(self.raw, dict) else {}
        return int(os.environ.get("MAX_CONCURRENT", settings.get("max_concurrent", 5)))


def _set_status(message: str, running: bool = True, stats: Optional[Dict[str, Any]] = None) -> None:
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        return

    try:
        import redis

        client = redis.Redis.from_url(redis_url)
        client.set(
            "scraper_status",
            json.dumps(
                {
                    "message": message,
                    "running": running,
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "stats": stats or {},
                }
            ),
            ex=3600,
        )
        client.close()
    except Exception as e:
        logger.warning("Status update failed: %s", e)


async def fast_scrape_all(config_dict: Dict[str, Any], cities: List[str], categories: List[str]) -> int:
    """Run the current DB-backed fast scrape flow for all city/category pairs."""
    from scraper import ContactScraper, load_config

    total_leads = 0
    scraper = ContactScraper(load_config())
    await scraper.init_db()

    try:
        for city in cities:
            for category in categories:
                _set_status(
                    f"Scraping {category} in {city}...",
                    True,
                    {"city": city, "category": category, "source": "AUTOMATOR", "total": total_leads},
                )
                count = await scraper.scrape_category_fast(city, category)
                total_leads += count
                logger.info("Extracted %s leads for %s / %s", count, city, category)

        _set_status(
            f"Complete: discovered {total_leads} leads.",
            False,
            {"source": "AUTOMATOR", "total": total_leads},
        )
        return total_leads
    except Exception as e:
        _set_status(f"Error: {e}", False, {"source": "AUTOMATOR"})
        raise
    finally:
        await scraper.close()


class ParallelScraper:
    """Small compatibility adapter used by worker.py."""

    def __init__(self, config: FastScraperConfig):
        self.config = config
        self.scraper = None

    async def init(self) -> None:
        from scraper import ContactScraper, load_config

        self.scraper = ContactScraper(load_config())
        await self.scraper.init_db()

    async def scrape_job(
        self,
        city: str,
        category: str,
        source_name: Optional[str] = None,
        page_num: int = 1,
        results_handler=None,
    ) -> int:
        if self.scraper is None:
            await self.init()

        return await self.scraper.scrape_category_fast(city, category, source_name)

    async def close(self) -> None:
        if self.scraper is not None:
            await self.scraper.close()
