import asyncio
import logging
import os
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import stealth_async
import asyncpg
import random
import time
from scrapers_registry import ScraperRegistry
from quality_pipeline import DataQualityPipeline

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
        """Setup an ultra-stealth context with randomized fingerprints."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]
        
        context = await self.browser.new_context(
            user_agent=random.choice(user_agents),
            viewport={'width': random.randint(1280, 1920), 'height': random.randint(720, 1080)},
            java_script_enabled=True,
            ignore_https_errors=True
        )
        
        # Apply stealth plugin
        # stealth_async(context) # Note: some versions of playwright-stealth take page, some context
        
        # Block heavy resources
        if self.config.block_resources:
            async def block_aggressively(route):
                if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
                    await route.abort()
                else:
                    await route.continue_()
            await context.route("**/*", block_aggressively)
            
        return context

    async def _human_navigation(self, page: Page):
        """Perform randomized human-like actions on the page."""
        try:
            # Random scroll down
            scroll_dist = random.randint(200, 800)
            await page.evaluate(f"window.scrollBy(0, {scroll_dist})")
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            # Subtle jitter
            await page.evaluate(f"window.scrollBy(0, {random.randint(-50, 50)})")
            
            # Thinking delay
            await asyncio.sleep(random.uniform(1.0, 3.0))
        except:
            pass

    async def scrape_job(self, city: str, category: str, source_name: str) -> int:
        """Run a single scraper job within the semaphore limit."""
        async with self.semaphore:
            scraper = ScraperRegistry.get(source_name)
            if not scraper:
                logger.error(f"No scraper found for {source_name}")
                return 0

            context = await self._setup_context()
            page = await context.new_page()
            
            # Trigger stealth on the page itself for better coverage
            await stealth_async(page)
            
            url = scraper.build_search_url(city, category)
            
            try:
                # Add a randomized "lead-in" delay to avoid concurrent spikes
                await asyncio.sleep(random.uniform(1, 4))
                
                await page.goto(url, timeout=self.config.timeout, wait_until='domcontentloaded')
                
                # Human behavior trigger
                await self._human_navigation(page)
                
                listings = await scraper.extract_listings(page, city, category)
                
                if listings:
                    # Enrich via pipeline
                    enriched = DataQualityPipeline.enrich_batch(listings)
                    await self._batch_insert(enriched, category, city, source_name)
                    return len(enriched)
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
                ON CONFLICT (phone_clean) DO UPDATE SET
                    name = COALESCE(contacts.name, EXCLUDED.name),
                    email = COALESCE(contacts.email, EXCLUDED.email),
                    address = COALESCE(contacts.address, EXCLUDED.address),
                    area = COALESCE(contacts.area, EXCLUDED.area),
                    city = COALESCE(contacts.city, EXCLUDED.city),
                    state = COALESCE(contacts.state, EXCLUDED.state),
                    arn = COALESCE(contacts.arn, EXCLUDED.arn),
                    quality_score = GREATEST(contacts.quality_score, EXCLUDED.quality_score),
                    quality_tier = CASE WHEN contacts.quality_score > EXCLUDED.quality_score THEN contacts.quality_tier ELSE EXCLUDED.quality_tier END,
                    enriched = TRUE
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
