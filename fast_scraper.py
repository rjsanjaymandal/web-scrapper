"""
Optimized Scraper Module
High-speed parallel scraping with concurrency control
"""

import asyncio
import logging
import os
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright
import asyncpg

logger = logging.getLogger(__name__)


class FastScraperConfig:
    """Configuration for fast scraping"""
    def __init__(self, config_dict: Dict):
        self.db_host = os.environ.get('DB_HOST', config_dict.get('db_host'))
        self.db_port = int(os.environ.get('DB_PORT', config_dict.get('db_port', 5432)))
        self.db_name = os.environ.get('DB_NAME', config_dict.get('db_name'))
        self.db_user = os.environ.get('DB_USER', config_dict.get('db_user'))
        self.db_password = os.environ.get('DB_PASSWORD', config_dict.get('db_password'))
        
        self.max_concurrent = int(os.environ.get('MAX_CONCURRENT', 3))
        self.batch_size = int(os.environ.get('BATCH_SIZE', 100))
        self.request_delay = float(os.environ.get('REQUEST_DELAY', 0.5))
        self.headless = os.environ.get('HEADLESS', 'true').lower() == 'true'
        self.timeout = int(os.environ.get('TIMEOUT', 30000))
        
        self.cities = config_dict.get('cities', [])
        self.categories = config_dict.get('categories', [])


class ParallelScraper:
    """
    High-speed parallel scraper with:
    - Concurrent browser instances
    - Batch database inserts
    - Rate limiting with semaphores
    - Connection pooling
    """
    
    def __init__(self, config: FastScraperConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.playwright = None
        self.browser = None
        self.pool = None
        
    async def init(self):
        """Initialize browser and DB pool"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.config.headless,
            args=['--disable-dev-shm-usage', '--no-sandbox']
        )
        
        self.pool = await asyncpg.create_pool(
            host=self.config.db_host,
            port=self.config.db_port,
            database=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
            min_size=5,
            max_size=20
        )
        logger.info("Initialized parallel scraper with concurrency=%s", self.config.max_concurrent)
        
    async def close(self):
        """Clean up resources"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.pool:
            await self.pool.close()
            
    async def scrape_with_semaphore(self, city: str, category: str, source: str) -> Dict:
        """Scrape with concurrency control"""
        async with self.semaphore:
            try:
                result = await self.scrape_single(city, category, source)
                return {"city": city, "category": category, "status": "ok", "count": result}
            except Exception as e:
                logger.error(f"Failed {city}/{category}: {e}")
                return {"city": city, "category": category, "status": "error", "error": str(e)}
    
    async def scrape_single(self, city: str, category: str, source: str) -> int:
        """Scrape a single city/category combination"""
        context = await self.browser.new_context()
        page = await context.new_page()
        
        listings = []
        try:
            if 'AMFI' in source:
                listings = await self._scrape_amfi(page, city)
            elif 'IRDAI' in source:
                listings = await self._scrape_irdai(page, city)
            elif 'ICAI' in source:
                listings = await self._scrape_icai(page, city)
            else:
                listings = await self._scrape_generic(page, city, category)
        finally:
            await page.close()
            await context.close()
        
        if listings:
            await self._batch_insert(listings, category, city, source)
        
        return len(listings)
    
    async def _scrape_amfi(self, page, city: str) -> List[Dict]:
        """Fast AMFI scraping"""
        url = f"https://www.amfiindia.com/locate-distributor"
        try:
            await page.goto(url, timeout=self.config.timeout)
            await page.wait_for_selector('table, div.results', timeout=5000)
            
            rows = await page.query_selector_all('tr, div.listing-item')
            listings = []
            
            for row in rows[:50]:
                try:
                    name = await row.query_selector('td:first-child, .name')
                    phone = await row.query_selector('td:nth-child(2), .phone')
                    
                    if name:
                        listings.append({
                            'name': (await name.inner_text()).strip(),
                            'phone': await self._extract_phone(await phone.inner_text() if phone else ''),
                            'city': city,
                            'source': 'AMFI'
                        })
                except:
                    continue
            
            return listings
        except Exception as e:
            logger.warning(f"AMFI scrape failed for {city}: {e}")
            return []
    
    async def _scrape_irdai(self, page, city: str) -> List[Dict]:
        """Fast IRDAI scraping"""
        url = "https://www.policyholder.gov.in/agent-search"
        try:
            await page.goto(url, timeout=self.config.timeout)
            await page.wait_for_selector('table, form', timeout=5000)
            
            rows = await page.query_selector_all('tr, .agent-card')
            listings = []
            
            for row in rows[:30]:
                try:
                    name = await row.query_selector('td, .name')
                    if name:
                        listings.append({
                            'name': (await name.inner_text()).strip(),
                            'city': city,
                            'source': 'IRDAI'
                        })
                except:
                    continue
            
            return listings
        except Exception as e:
            logger.warning(f"IRDAI scrape failed for {city}: {e}")
            return []
    
    async def _scrape_icai(self, page, city: str) -> List[Dict]:
        """Fast ICAI scraping"""
        url = f"https://caconnect.icai.org/city-wise-list/{city}"
        try:
            await page.goto(url, timeout=self.config.timeout)
            await page.wait_for_selector('.members, .firms', timeout=5000)
            
            cards = await page.query_selector_all('.member-card, .ca-card')
            listings = []
            
            for card in cards[:50]:
                try:
                    name = await card.query_selector('.name, h3')
                    email = await card.query_selector('.email, .mail')
                    
                    if name:
                        listings.append({
                            'name': (await name.inner_text()).strip(),
                            'email': (await email.inner_text()).strip() if email else '',
                            'city': city,
                            'source': 'ICAI'
                        })
                except:
                    continue
            
            return listings
        except Exception as e:
            logger.warning(f"ICAI scrape failed for {city}: {e}")
            return []
    
    async def _scrape_generic(self, page, city: str, category: str) -> List[Dict]:
        """Generic fallback scraper"""
        return []
    
    async def _extract_phone(self, text: str) -> str:
        """Extract clean phone number"""
        digits = re.sub(r'[^\d]', '', text)
        if len(digits) >= 10:
            return digits[-10:]
        return text
    
    async def _batch_insert(self, listings: List[Dict], category: str, city: str, source: str):
        """Fast batch insert to database"""
        if not listings:
            return
        
        # Filter valid contacts (has phone or email)
        valid = [
            l for l in listings 
            if (l.get('phone') and l.get('phone').strip()) or 
               (l.get('email') and l.get('email').strip())
        ]
        
        if not valid:
            return
        
        records = [
            (
                rec.get('name', ''),
                rec.get('phone', ''),
                rec.get('email', ''),
                rec.get('address', ''),
                category,
                city,
                rec.get('area', ''),
                rec.get('state', ''),
                source,
                rec.get('url', ''),
                rec.get('phone', '')[-10:] if rec.get('phone') else None,
                bool(re.match(r'[\w.%+-]+@[\w.-]+\.\w+', rec.get('email', ''))) if rec.get('email') else False,
                False,
                rec.get('arn', ''),
                rec.get('license_no', ''),
                rec.get('membership_no', ''),
            )
            for rec in valid
        ]
        
        async with self.pool.acquire() as conn:
            await conn.executemany('''
                INSERT INTO contacts (name, phone, email, address, category, city, area, state, source, source_url, phone_clean, email_valid, enriched, arn, license_no, membership_no)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT DO NOTHING
            ''', records)
        
        logger.info(f"Batch inserted {len(valid)} records for {city}/{category}")
    
    async def run_parallel(self, jobs: List[tuple]) -> List[Dict]:
        """Run multiple scrape jobs in parallel"""
        logger.info(f"Starting parallel scrape with {len(jobs)} jobs, concurrency={self.config.max_concurrent}")
        
        tasks = [
            self.scrape_with_semaphore(city, category, source) 
            for city, category, source in jobs
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        ok_count = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'ok')
        logger.info(f"Parallel scrape complete: {ok_count}/{len(jobs)} successful")
        
        return results


async def fast_scrape_all(config_dict: Dict, cities: List[str], categories: List[str], sources: List[str] = None):
    """
    High-speed parallel scraping function
    
    Usage:
        results = await fast_scrape_all(
            config_dict={'db_host': '...', 'cities': ['Mumbai', 'Delhi']},
            cities=['Mumbai', 'Delhi', 'Bangalore'],
            categories=['Mutual-Fund-Agents', 'Insurance-Agents']
        )
    """
    config = FastScraperConfig(config_dict)
    scraper = ParallelScraper(config)
    
    await scraper.init()
    
    try:
        jobs = []
        for city in cities:
            for category in categories:
                source = 'AMFI' if 'mutual' in category.lower() else \
                        'IRDAI' if 'insurance' in category.lower() else \
                        'ICAI'
                jobs.append((city, category, source))
        
        results = await scraper.run_parallel(jobs)
        return results
    finally:
        await scraper.close()


class FastScrapeTask:
    """Celery-compatible fast scrape task"""
    
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        
    async def execute(self, cities: List[str], categories: List[str]) -> Dict:
        """Execute fast scrape"""
        from tasks import _load_runtime_config
        
        config_dict = _load_runtime_config()
        config = FastScraperConfig(config_dict)
        config.max_concurrent = self.max_concurrent
        
        scraper = ParallelScraper(config)
        await scraper.init()
        
        try:
            jobs = []
            for city in cities:
                for category in categories:
                    source = self._get_source(category)
                    jobs.append((city, category, source))
            
            results = await scraper.run_parallel(jobs)
            
            return {
                'status': 'completed',
                'jobs': len(jobs),
                'results': results
            }
        finally:
            await scraper.close()
    
    def _get_source(self, category: str) -> str:
        """Map category to source"""
        cat_lower = category.lower()
        if 'mutual' in cat_lower:
            return 'AMFI'
        elif 'insurance' in cat_lower:
            return 'IRDAI'
        elif 'tax' in cat_lower or 'chartered' in cat_lower:
            return 'ICAI'
        return 'Unknown'
