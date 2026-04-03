import asyncio
import random
import yaml
import asyncpg
import csv
import logging
import os
import re
import aiohttp
import json
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Page, Browser, Playwright
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJ_DIR = Path(__file__).parent
EXPORTS_DIR = PROJ_DIR / "exports"
LOGS_DIR = PROJ_DIR / "logs"
EXPORTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


@dataclass
class Config:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    proxies: List[Dict]
    request_delay_min: int
    request_delay_max: int
    max_retries: int
    timeout_seconds: int
    headless: bool
    test_mode: bool
    export_csv: bool
    csv_output_dir: str
    enable_email_extraction: bool
    enable_sitemap: bool
    enable_deduplication: bool
    enable_email_verify: bool
    enable_enrichment: bool
    scheduler_enabled: bool
    scheduler_interval_hours: int
    categories: List[str]
    cities: List[str]


def load_config() -> Config:
    with open('config.yaml', 'r') as f:
        data = yaml.safe_load(f)
    
    scraper_cfg = data.get('scraper', {})
    
    proxies = []
    if 'proxies' in data.get('proxy', {}):
        for p in data['proxy']['proxies']:
            proxies.append({
                'host': p.get('host', ''),
                'username': p.get('username', ''),
                'password': p.get('password', '')
            })
    elif data.get('proxy', {}).get('host'):
        proxies.append({
            'host': data['proxy']['host'],
            'username': data['proxy'].get('username', ''),
            'password': data['proxy'].get('password', '')
        })
    
    return Config(
        db_host=data['database']['host'],
        db_port=data['database']['port'],
        db_name=data['database']['name'],
        db_user=data['database']['user'],
        db_password=data['database']['password'],
        proxies=proxies,
        request_delay_min=scraper_cfg.get('request_delay_min', 2),
        request_delay_max=scraper_cfg.get('request_delay_max', 5),
        max_retries=scraper_cfg.get('max_retries', 3),
        timeout_seconds=scraper_cfg.get('timeout_seconds', 30),
        headless=scraper_cfg.get('headless', True),
        test_mode=scraper_cfg.get('test_mode', False),
        export_csv=scraper_cfg.get('export_csv', True),
        csv_output_dir=scraper_cfg.get('csv_output_dir', 'exports'),
        enable_email_extraction=scraper_cfg.get('enable_email_extraction', True),
        enable_sitemap=scraper_cfg.get('enable_sitemap', False),
        enable_deduplication=scraper_cfg.get('enable_deduplication', True),
        enable_email_verify=scraper_cfg.get('enable_email_verify', False),
        enable_enrichment=scraper_cfg.get('enable_enrichment', False),
        scheduler_enabled=scraper_cfg.get('scheduler_enabled', False),
        scheduler_interval_hours=scraper_cfg.get('scheduler_interval_hours', 24),
        categories=data['categories'],
        cities=data['cities']
    )


def save_progress(city: str, category: str, source: str, page: int):
    progress_file = LOGS_DIR / "progress.json"
    progress = {}
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
    progress[f"{source}_{category}_{city}"] = {'page': page, 'last_updated': datetime.now().isoformat()}
    progress_file.write_text(json.dumps(progress))


def load_progress(city: str, category: str, source: str) -> int:
    progress_file = LOGS_DIR / "progress.json"
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
        key = f"{source}_{category}_{city}"
        if key in progress:
            return progress[key].get('page', 1)
    return 1


class EmailVerifier:
    EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    @staticmethod
    def extract_from_text(text: str) -> Optional[str]:
        match = EmailVerifier.EMAIL_REGEX.search(text)
        return match.group(0) if match else None
    
    @staticmethod
    async def verify_email(email: str) -> bool:
        if not email:
            return False
        domain = email.split('@')[1] if '@' in email else None
        if not domain:
            return False
        valid_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'rediffmail.com']
        if domain.lower() in valid_domains:
            return True
        return True


class DataEnricher:
    @staticmethod
    async def enrich_contact(contact: Dict) -> Dict:
        contact['enriched'] = False
        contact['verified'] = False
        
        if contact.get('phone'):
            phone = re.sub(r'[^\d]', '', contact['phone'])
            if len(phone) >= 10:
                contact['phone_clean'] = phone[-10:]
        
        if contact.get('email'):
            contact['email_valid'] = bool(EmailVerifier.EMAIL_REGEX.match(contact['email']))
        
        return contact


class BaseScraper(ABC):
    @abstractmethod
    async def extract_listings(self, page: Page) -> List[Dict]:
        pass
    
    @abstractmethod
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        pass
    
    @abstractmethod
    async def get_detail_url(self, card) -> Optional[str]:
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        pass


class JustDialScraper(BaseScraper):
    source_name = "JustDial"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        category_slug = category.lower().replace(' ', '-')
        if page > 1:
            return f"https://www.justdial.com/{city}/{category_slug}/page-{page}"
        return f"https://www.justdial.com/{city}/{category_slug}"
    
    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a.store-name')
            if link:
                href = await link.get_attribute('href')
                return href
        except:
            pass
        return None
    
    async def extract_listings(self, page: Page) -> List[Dict]:
        listings = []
        try:
            await page.wait_for_selector('.store-list', timeout=10000)
            cards = await page.query_selector_all('.store-list .store-info')
            
            for card in cards:
                try:
                    name = await self._get_text(card, '.store-name')
                    phone = await self._get_text(card, '.store-phone')
                    address = await self._get_text(card, '.store-address')
                    area = await self._get_text(card, '.store-area')
                    detail_url = await self.get_detail_url(card)
                    
                    if name:
                        listings.append({
                            'name': name.strip(),
                            'phone': self._clean_phone(phone) if phone else None,
                            'address': address.strip() if address else None,
                            'area': area.strip() if area else None,
                            'detail_url': detail_url
                        })
                except Exception as e:
                    logger.debug(f"Card parse error: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Listings extraction error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None
    
    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class IndiaMartScraper(BaseScraper):
    source_name = "IndiaMart"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        category_slug = category.lower().replace(' ', '-')
        city_slug = city.lower().replace(' ', '-')
        return f"https://www.indiamart.com/proddir/{category_slug}-in-{city_slug}/?pn={page}"
    
    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('.prod-name a')
            if link:
                href = await link.get_attribute('href')
                return href
        except:
            pass
        return None
    
    async def extract_listings(self, page: Page) -> List[Dict]:
        listings = []
        try:
            cards = await page.query_selector_all('.prod-list .prod-item')
            
            for card in cards:
                try:
                    name = await self._get_text(card, '.prod-name')
                    phone = await self._get_text(card, '.prod-phn')
                    address = await self._get_text(card, '.prod-addr')
                    detail_url = await self.get_detail_url(card)
                    
                    if name:
                        listings.append({
                            'name': name.strip(),
                            'phone': self._clean_phone(phone) if phone else None,
                            'address': address.strip() if address else None,
                            'area': None,
                            'detail_url': detail_url
                        })
                except:
                    continue
        except Exception as e:
            logger.warning(f"IndiaMart extraction error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None
    
    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class ICICIScraper(BaseScraper):
    source_name = "ICICI"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        category_lower = category.lower().replace(' ', '-')
        return f"https://www.iciciprulife.com/agentsearch/{category_lower}.do?city={city.lower()}"
    
    async def get_detail_url(self, card) -> Optional[str]:
        return None
    
    async def extract_listings(self, page: Page) -> List[Dict]:
        listings = []
        try:
            cards = await page.query_selector_all('.agent-card, .search-result-item')
            
            for card in cards:
                try:
                    name = await self._get_text(card, '.agent-name, .name')
                    phone = await self._get_text(card, '.agent-phone, .phone')
                    address = await self._get_text(card, '.agent-address, .address')
                    
                    if name:
                        listings.append({
                            'name': name.strip(),
                            'phone': self._clean_phone(phone) if phone else None,
                            'address': address.strip() if address else None,
                            'area': None,
                            'detail_url': None
                        })
                except:
                    continue
        except Exception as e:
            logger.warning(f"ICICI extraction error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None
    
    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class SitemapScraper:
    @staticmethod
    async def fetch_sitemap_urls(base_url: str) -> List[str]:
        urls = []
        sitemap_urls = [f"{base_url}/sitemap.xml"]
        
        for sitemap_url in sitemap_urls:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(sitemap_url, timeout=10) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            url_matches = re.findall(r'<loc>(.*?)</loc>', text, re.IGNORECASE)
                            urls.extend(url_matches)
            except Exception as e:
                logger.debug(f"Sitemap fetch error: {e}")
        
        return urls


class ProxyManager:
    def __init__(self, proxies: List[Dict], test_mode: bool):
        self.proxies = proxies
        self.test_mode = test_mode
        self.current_index = 0
    
    def get_proxy(self) -> Optional[Dict]:
        if self.test_mode or not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy
    
    def get_proxy_string(self) -> Optional[str]:
        proxy = self.get_proxy()
        if not proxy:
            return None
        if proxy.get('username') and proxy.get('password'):
            return f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}"
        return f"http://{proxy['host']}"


class RateLimiter:
    def __init__(self, min_delay: int, max_delay: int):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.base_delay = 2
        self.consecutive_errors = 0
    
    async def wait(self):
        if self.consecutive_errors > 3:
            delay = self.base_delay * 2
        else:
            delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)
    
    def record_success(self):
        self.consecutive_errors = 0
        self.base_delay = max(1, self.base_delay - 0.1)
    
    def record_failure(self):
        self.consecutive_errors += 1
        self.base_delay = min(30, self.base_delay + 1)


class ContactScraper:
    def __init__(self, config: Config):
        self.config = config
        self.browser: Optional[Browser] = None
        self.playwright: Optional[Playwright] = None
        self.context = None
        self.page = None
        self.pool: Optional[asyncpg.Pool] = None
        self.proxy_manager = ProxyManager(config.proxies, config.test_mode)
        self.rate_limiter = RateLimiter(config.request_delay_min, config.request_delay_max)
        
        self.scrapers: List[BaseScraper] = [
            JustDialScraper(),
            IndiaMartScraper(),
            ICICIScraper()
        ]
        
        self.stats = {
            'total_scrape': 0,
            'successful': 0,
            'failed': 0,
            'duplicates_skipped': 0,
            'by_source': {}
        }

    async def init_db(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                min_size=2,
                max_size=10
            )
            
            await self.pool.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    phone VARCHAR(50),
                    email VARCHAR(255),
                    address TEXT,
                    category VARCHAR(100),
                    city VARCHAR(100),
                    area VARCHAR(100),
                    source VARCHAR(100),
                    source_url TEXT,
                    phone_clean VARCHAR(50),
                    email_valid BOOLEAN,
                    enriched BOOLEAN,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await self.pool.execute('''
                CREATE TABLE IF NOT EXISTS scrape_logs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(100),
                    category VARCHAR(100),
                    city VARCHAR(100),
                    status VARCHAR(50),
                    records_count INTEGER,
                    error_message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
            ''')
            
            await self.pool.execute('''
                CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone_clean)
            ''')
            await self.pool.execute('''
                CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email)
            ''')
            await self.pool.execute('''
                CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)
            ''')
            await self.pool.execute('''
                CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)
            ''')
            
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database init failed: {e}")
            raise

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu'
        ]
        
        proxy_str = self.proxy_manager.get_proxy_string()
        
        if self.config.test_mode:
            logger.info("Running in TEST MODE (no proxy)")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                args=launch_args
            )
        else:
            logger.info(f"Using proxy: {proxy_str[:50] if proxy_str else 'None'}...")
            self.browser = await self.playwright.chromium.launch(
                headless=self.config.headless,
                args=launch_args
            )
            
        proxy_dict = {'server': proxy_str} if proxy_str else None
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            proxy=proxy_dict
        )
        
        self.page = await self.context.new_page()
        
        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)
        
        logger.info("Browser initialized")

    async def extract_email_from_detail(self, detail_url: str) -> Optional[str]:
        if not detail_url or not self.config.enable_email_extraction:
            return None
            
        try:
            await self.page.goto(detail_url, timeout=self.config.timeout_seconds * 1000, wait_until='networkidle')
            await asyncio.sleep(1)
            
            page_text = await self.page.content()
            email = EmailVerifier.extract_from_text(page_text)
            
            return email
        except Exception as e:
            logger.debug(f"Email extraction failed: {e}")
            return None

    async def is_duplicate(self, phone: str, email: str) -> bool:
        if not self.config.enable_deduplication:
            return False
        
        async with self.pool.acquire() as conn:
            if phone:
                exists = await conn.fetchval('''
                    SELECT 1 FROM contacts WHERE phone_clean = $1 LIMIT 1
                ''', phone)
                if exists:
                    return True
            
            if email:
                exists = await conn.fetchval('''
                    SELECT 1 FROM contacts WHERE email = $1 LIMIT 1
                ''', email)
                if exists:
                    return True
        
        return False

    async def scrape_page(self, url: str, max_pages: int = 3) -> List[Dict]:
        all_listings = []
        
        for page_num in range(1, max_pages + 1):
            retries = 0
            success = False
            
            while retries < self.config.max_retries and not success:
                try:
                    page_url = url if page_num == 1 else url.replace('.com/', f'.com/page-{page_num}/')
                    logger.debug(f"Fetching: {page_url}")
                    
                    await self.page.goto(page_url, timeout=self.config.timeout_seconds * 1000, wait_until='networkidle')
                    await self.rate_limiter.wait()
                    
                    listings = await self._extract_current_page()
                    
                    if not listings:
                        break
                    
                    for listing in listings:
                        if self.config.enable_email_extraction and listing.get('detail_url'):
                            email = await self.extract_email_from_detail(listing['detail_url'])
                            listing['email'] = email
                        
                        if self.config.enable_enrichment:
                            listing = await DataEnricher.enrich_contact(listing)
                        
                        is_dup = await self.is_duplicate(listing.get('phone_clean'), listing.get('email'))
                        if is_dup:
                            self.stats['duplicates_skipped'] += 1
                            continue
                        
                        all_listings.append(listing)
                    
                    success = True
                    self.rate_limiter.record_success()
                    self.stats['successful'] += 1
                    
                except Exception as e:
                    retries += 1
                    self.rate_limiter.record_failure()
                    self.stats['failed'] += 1
                    logger.warning(f"Retry {retries}/{self.config.max_retries}: {e}")
                    await asyncio.sleep(random.uniform(3, 8))
                    
            if not success:
                logger.error(f"Failed after {self.config.max_retries} retries")
                
        return all_listings

    async def _extract_current_page(self) -> List[Dict]:
        listings = []
        try:
            url_lower = self.page.url.lower()
            if 'justdial' in url_lower:
                scraper = JustDialScraper()
            elif 'indiamart' in url_lower:
                scraper = IndiaMartScraper()
            elif 'icici' in url_lower:
                scraper = ICICIScraper()
            else:
                scraper = JustDialScraper()
            
            listings = await scraper.extract_listings(self.page)
        except Exception as e:
            logger.warning(f"Extraction error: {e}")
        return listings

    async def save_to_db(self, listings: List[Dict], category: str, city: str, source: str, url: str):
        if not listings:
            return
            
        async with self.pool.acquire() as conn:
            for listing in listings:
                await conn.execute('''
                    INSERT INTO contacts (name, phone, email, address, category, city, area, source, source_url, phone_clean, email_valid, enriched)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ''', listing.get('name'), listing.get('phone'), listing.get('email'), listing.get('address'),
                    category, city, listing.get('area'), source, url, 
                    listing.get('phone_clean'), listing.get('email_valid', False), listing.get('enriched', False))
        
        logger.info(f"Saved {len(listings)} records to database")

    async def export_to_csv(self, source: Optional[str] = None):
        os.makedirs(self.config.csv_output_dir, exist_ok=True)
        
        async with self.pool.acquire() as conn:
            if source:
                rows = await conn.fetch('''
                    SELECT * FROM contacts WHERE source = $1 ORDER BY scraped_at DESC
                ''', source)
            else:
                rows = await conn.fetch('SELECT * FROM contacts ORDER BY scraped_at DESC')
            
            if not rows:
                logger.warning("No data to export")
                return
            
            filename = f"{self.config.csv_output_dir}/contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))
            
            logger.info(f"Exported {len(rows)} records to {filename}")
            return filename

    async def get_stats(self) -> Dict:
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
            by_source = await conn.fetch('''
                SELECT source, COUNT(*) as count FROM contacts GROUP BY source
            ''')
            by_category = await conn.fetch('''
                SELECT category, COUNT(*) as count FROM contacts GROUP BY category
            ''')
            with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL")
            
            return {
                'total_contacts': total,
                'with_email': with_email,
                'by_source': {r['source']: r['count'] for r in by_source},
                'by_category': {r['category']: r['count'] for r in by_category}
            }

    async def scrape_category(self, city: str, category: str, source_name: Optional[str] = None):
        logger.info(f"\n>>> Scraping: {category} in {city}")
        
        scrapers_to_run = self.scrapers
        if source_name:
            scrapers_to_run = [s for s in self.scrapers if s.source_name == source_name]
            
        for scraper in scrapers_to_run:
            url = scraper.build_search_url(city, category)
            logger.info(f"Source: {scraper.source_name}")
            
            self.stats['total_scrape'] += 1
            listings = await self.scrape_page(url)
            
            await self.save_to_db(listings, category, city, scraper.source_name, url)
            
            self.stats['by_source'][scraper.source_name] = \
                self.stats['by_source'].get(scraper.source_name, 0) + len(listings)
            
            save_progress(city, category, scraper.source_name, 1)
            await self.rate_limiter.wait()

    async def run(self):
        start_time = datetime.now()
        logger.info("="*60)
        logger.info("Starting Contact Scraper - Enhanced Version")
        logger.info("="*60)
        
        await self.init_db()
        await self.init_browser()
        
        try:
            for city in self.config.cities:
                for category in self.config.categories:
                    await self.scrape_category(city, category)
            
            if self.config.export_csv:
                await self.export_to_csv()
            
            stats = await self.get_stats()
            elapsed = datetime.now() - start_time
            logger.info("\n" + "="*60)
            logger.info("SCRAPING COMPLETE")
            logger.info(f"Total contacts: {stats['total_contacts']}")
            logger.info(f"With email: {stats['with_email']}")
            logger.info(f"By source: {stats['by_source']}")
            logger.info(f"By category: {stats['by_category']}")
            logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
            logger.info(f"Time elapsed: {elapsed}")
            logger.info("="*60)
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
        finally:
            await self.close()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.pool:
            await self.pool.close()


class Scheduler:
    def __init__(self, config: Config, interval_hours: int):
        self.config = config
        self.interval_hours = interval_hours
    
    async def run_continuously(self):
        logger.info(f"Scheduler started - running every {self.interval_hours} hours")
        
        while True:
            scraper = ContactScraper(self.config)
            await scraper.run()
            
            logger.info(f"Waiting {self.interval_hours} hours before next run...")
            await asyncio.sleep(self.interval_hours * 3600)


async def main():
    config = load_config()
    
    if config.scheduler_enabled:
        scheduler = Scheduler(config, config.scheduler_interval_hours)
        await scheduler.run_continuously()
    else:
        scraper = ContactScraper(config)
        await scraper.run()


if __name__ == '__main__':
    asyncio.run(main())