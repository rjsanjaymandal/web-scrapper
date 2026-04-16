import random
import asyncio
import logging
import os
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext
import asyncpg

# Trigger scraper registration before using ScraperRegistry
import scraper  # noqa: F401
import enhanced_utils  # noqa: F401

from scrapers_registry import ScraperRegistry
from processing import ProcessingHandler
from stealth_utils import StealthManager


logger = logging.getLogger(__name__)


class FastScraperConfig:
    def __init__(self, config_dict: Dict):
        self.db_url = os.environ.get("DATABASE_URL")
        if self.db_url and self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)

        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", 20))
        self.headless = os.environ.get("HEADLESS", "true").lower() == "true"
        self.timeout = int(os.environ.get("TIMEOUT", 60000))
        self.block_resources = True

        self.cities = config_dict.get("cities", [])
        self.categories = config_dict.get("categories", [])
        
        # Security: Prefer environment variables for proxies (Railway best practice)
        self.proxy_list = []
        env_proxy_host = os.environ.get("PROXY_HOST")
        if env_proxy_host:
            env_proxy_port = os.environ.get("PROXY_PORT", "")
            # Build host:port string if port is provided separately
            if env_proxy_port and ":" not in env_proxy_host:
                proxy_host = f"{env_proxy_host}:{env_proxy_port}"
            else:
                proxy_host = env_proxy_host
            self.proxy_list.append({
                "host": proxy_host,
                "username": os.environ.get("PROXY_USER"),
                "password": os.environ.get("PROXY_PASS")
            })
            # Validate port is present
            if ":" not in proxy_host.split("//")[-1]:
                logger.warning(f"⚠️  PROXY HAS NO PORT! Host='{proxy_host}'. Set PROXY_PORT env var.")
            logger.info(f"✅ Proxy configured: {proxy_host}")
        else:
            self.proxy_list = config_dict.get("proxies", [])


class ParallelScraper:
    """Efficient parallel engine using Playwright & asyncpg."""

    def __init__(self, config: FastScraperConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.playwright = None
        self.browser = None
        self.pool = None

    async def init(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.config.headless,
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-extensions",
            ],
        )

        if self.config.db_url:
            self.pool = await asyncpg.create_pool(
                dsn=self.config.db_url, min_size=5, max_size=self.config.max_concurrent + 5
            )
        else:
            logger.error("No DATABASE_URL found for parallel scraper pool.")

        logger.info(f"Parallel Engine V2 ready: Concurrency={self.config.max_concurrent}")

        # Proxy connectivity smoke test
        if self.config.proxy_list:
            await self._test_proxy_connectivity()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.pool:
            await self.pool.close()

    async def _test_proxy_connectivity(self):
        """Quick smoke test: one page load through the proxy to validate connectivity."""
        p = self.config.proxy_list[0]
        host = p["host"]
        if not host.startswith("http"):
            host = f"http://{host}"
        proxy_config = {
            "server": host,
            "username": p.get("username"),
            "password": p.get("password"),
        }
        logger.info(f"🔍 Testing proxy: server={host}")
        ctx = None
        try:
            ctx = await self.browser.new_context(
                proxy=proxy_config,
                ignore_https_errors=True,
            )
            page = await ctx.new_page()
            resp = await page.goto("https://httpbin.org/ip", timeout=30000, wait_until="domcontentloaded")
            body = await page.inner_text("body")
            logger.info(f"✅ PROXY OK! Response status={resp.status}, IP={body.strip()[:80]}")
        except Exception as e:
            logger.critical(f"❌ PROXY FAILED! Error: {e}")
            logger.critical(f"❌ Check PROXY_HOST (must include port) and PROXY_USER/PROXY_PASS env vars!")
        finally:
            if ctx:
                await ctx.close()

    async def _setup_context(self) -> BrowserContext:
        user_agent = StealthManager.get_random_ua()
        extra_headers = StealthManager.get_modern_headers(user_agent)

        # Selection of proxy if available
        proxy_config = None
        if self.config.proxy_list:
            p = random.choice(self.config.proxy_list)
            host = p["host"]
            # Playwright requires full URL format: http://host:port
            if not host.startswith("http"):
                host = f"http://{host}"
            proxy_config = {
                "server": host,
                "username": p.get("username"),
                "password": p.get("password")
            }
            logger.debug(f"Worker using proxy: {host}")

        context = await self.browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers,
            viewport={"width": 1280, "height": 720},
            device_scale_factor=random.choice([1, 2]),
            has_touch=random.choice([True, False]),
            ignore_https_errors=True,
            proxy=proxy_config,
        )

        # Apply advanced stealth patches
        await StealthManager.apply_stealth(context)

        # Block heavy and tracking resources
        if self.config.block_resources:
            # Domain blocklist for trackers/analytics
            block_domains = [
                "google-analytics.com",
                "googletagmanager.com",
                "facebook.net",
                "hotjar.com",
                "clarity.ms",
                "doubleclick.net",
                "adnxs.com",
            ]

            async def block_aggressively(route):
                url = route.request.url.lower()
                if route.request.resource_type in [
                    "image",
                    "font",
                    "media",
                ] or any(domain in url for domain in block_domains):
                    await route.abort()
                elif route.request.resource_type == "stylesheet":
                    # Only block large sheets or non-critical ones? 
                    # For now, allow CSS to avoid layout break detection by some sites
                    await route.continue_()
                else:
                    await route.continue_()

            await context.route("**/*", block_aggressively)

        return context

    async def scrape_job(self, city: str, category: str, source_name: str) -> int:
        """Run a single scraper job within the semaphore limit."""
        async with self.semaphore:
            scraper = ScraperRegistry.get(source_name)
            if not scraper:
                logger.error(f"No scraper found for {source_name}")
                return 0

            context = await self._setup_context()
            page = await context.new_page()
            url = scraper.build_search_url(city, category)

            try:
                await page.goto(
                    url, timeout=self.config.timeout, wait_until="domcontentloaded"
                )
                listings = await scraper.extract_listings(page, city, category)
                logger.info(f"Job: {source_name} | Raw Extracted: {len(listings)} leads")

                if not listings:
                    # Debug: Why was it 0?
                    title = await page.title()
                    body = await page.inner_text("body")
                    logger.warning(f"Job: {source_name} | 0 Leads | Page Title: {title} | Body Snippet: {body[:200].replace('\\n', ' ')}")

                if listings:
                    # Filter and Process via unified handler
                    processed = ProcessingHandler.process_batch(listings)
                    logger.info(f"Job: {source_name} | Processed: {len(processed)} leads")
                    
                    valid = ProcessingHandler.filter_valid(processed)
                    logger.info(f"Job: {source_name} | Valid: {len(valid)} leads")
                    
                    if valid:
                        await self._batch_insert(valid, category, city, source_name)
                    return len(valid)
                return 0
            except Exception as e:
                logger.error(
                    f"Job failed: {source_name} | {city}/{category} | Error: {e}"
                )
                return 0
            finally:
                await page.close()
                await context.close()

    async def _batch_insert(
        self, listings: List[Dict], category: str, city: str, source: str
    ):
        if not self.pool:
            return

        records = [
            (
                rec.get("name", "")[:255],
                rec.get("phone", "")[:50],
                rec.get("email", "")[:255],
                rec.get("address", ""),
                category[:100],
                city[:100],
                rec.get("area", "")[:100],
                rec.get("state", "")[:100],
                source[:100],
                rec.get("detail_url", ""),
                rec.get("phone_clean", "")[:50],
                rec.get("email_valid", False),
                True,  # enriched
                rec.get("arn", "")[:50],
                rec.get("license_no", "")[:100],
                rec.get("membership_no", "")[:100],
                rec.get("quality_score", 0),
                rec.get("quality_tier", "low"),
            )
            for rec in listings
            if rec.get("name")
        ]

        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO contacts (
                    name, phone, email, address, category, city, area, state, source, 
                    source_url, phone_clean, email_valid, enriched, arn, license_no, 
                    membership_no, quality_score, quality_tier
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL
                DO UPDATE SET
                    quality_score = EXCLUDED.quality_score,
                    quality_tier = EXCLUDED.quality_tier,
                    scraped_at = EXCLUDED.scraped_at,
                    enriched = TRUE
                WHERE EXCLUDED.quality_score >= contacts.quality_score
            """,
                records,
            )

    async def run_parallel_suite(self, jobs: List[tuple]):
        tasks = [self.scrape_job(city, cat, src) for city, cat, src in jobs]
        results = await asyncio.gather(*tasks)
        total_leads = sum(results)
        logger.info(f"Parallel Suite Complete: Total Leads Found: {total_leads}")
        return total_leads


async def fast_scrape_all(config_dict: Dict, cities: List[str], categories: List[str]):
    config = FastScraperConfig(config_dict)
    engine = ParallelScraper(config)
    await engine.init()

    try:
        jobs = []
        for city in cities:
            for cat in categories:
                # Get ALL sources for this category, not just one
                sources = ScraperRegistry.get_all_sources_for_category(cat)
                for source in sources:
                    jobs.append((city, cat, source))

        logger.info(
            f"Total scraping jobs: {len(jobs)} (cities: {len(cities)}, categories: {len(categories)})"
        )
        return await engine.run_parallel_suite(jobs)
    finally:
        await engine.close()
