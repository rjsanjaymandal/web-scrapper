import asyncio
import logging
import os
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext
import asyncpg
from scrapers_registry import ScraperRegistry
from processing import ProcessingHandler
from stealth_utils import StealthManager


logger = logging.getLogger(__name__)

class FastScraperConfig:
    def __init__(self, config_dict: Dict):
        self.db_url = os.environ.get('DATABASE_URL')
        if self.db_url and self.db_url.startswith('postgres://'):
            self.db_url = self.db_url.replace('postgres://', 'postgresql://', 1)
            
        self.max_concurrent = int(os.environ.get('MAX_CONCURRENT', 5))
        self.headless = os.environ.get('HEADLESS', 'true').lower() == 'true'
        self.timeout = int(os.environ.get('TIMEOUT', 60000))
        self.block_resources = True
        
        self.cities = config_dict.get('cities', [])
        self.categories = config_dict.get('categories', [])

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
            args=['--disable-dev-shm-usage', '--no-sandbox']
        )
        
        if self.config.db_url:
            self.pool = await asyncpg.create_pool(dsn=self.config.db_url, min_size=5, max_size=20)
        else:
            logger.error("No DATABASE_URL found for parallel scraper pool.")
            
        logger.info(f"Parallel engine ready: Concurrency={self.config.max_concurrent}")
        
    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
        if self.pool: await self.pool.close()

    async def _setup_context(self) -> BrowserContext:
        user_agent = StealthManager.get_random_ua()
        extra_headers = StealthManager.get_modern_headers(user_agent)
        
        context = await self.browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers,
            viewport={'width': 1280, 'height': 720}
        )
        
        # Apply advanced stealth patches
        await StealthManager.apply_stealth(context)
        
        # Block heavy resources

        if self.config.block_resources:
            async def block_aggressively(route):
                if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
                    await route.abort()
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
                await page.goto(url, timeout=self.config.timeout, wait_until='domcontentloaded')
                listings = await scraper.extract_listings(page, city, category)
                
                if listings:
                    # Filter and Process via unified handler
                    processed = ProcessingHandler.process_batch(listings)
                    valid = ProcessingHandler.filter_valid(processed)
                    if valid:
                        await self._batch_insert(valid, category, city, source_name)
                    return len(valid)
                return 0
            except Exception as e:
                logger.error(f"Job failed: {source_name} | {city}/{category} | Error: {e}")
                return 0
            finally:
                await page.close()
                await context.close()

    async def _batch_insert(self, listings: List[Dict], category: str, city: str, source: str):
        if not self.pool: return
        
        records = [
            (
                rec.get('name', '')[:255],
                rec.get('phone', '')[:50],
                rec.get('email', '')[:255],
                rec.get('address', ''),
                category[:100],
                city[:100],
                rec.get('area', '')[:100],
                rec.get('state', '')[:100],
                source[:100],
                rec.get('detail_url', ''),
                rec.get('phone_clean', '')[:50],
                rec.get('email_valid', False),
                True, # enriched
                rec.get('arn', '')[:50],
                rec.get('license_no', '')[:100],
                rec.get('membership_no', '')[:100],
                rec.get('quality_score', 0),
                rec.get('quality_tier', 'low')
            )
            for rec in listings if rec.get('name')
        ]
        
        async with self.pool.acquire() as conn:
            await conn.executemany('''
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
            ''', records)

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
                source = ScraperRegistry.get_source_for_category(cat)
                jobs.append((city, cat, source))
        
        return await engine.run_parallel_suite(jobs)
    finally:
        await engine.close()
