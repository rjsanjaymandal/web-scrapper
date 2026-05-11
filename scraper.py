import asyncio
import random
import yaml

from dorking_scraper import GoogleDorkScraper
import asyncpg
import csv
import logging
import os
import re
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
# from playwright.async_api import async_playwright, Page, Browser, Playwright
Page = dict # Mock type for disabled Playwright
Browser = Any # Mock type for disabled Playwright
Playwright = Any # Mock type for disabled Playwright
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
from raw_storage import storage
from scrapers.base import BaseScraper, ScraperRegistry
from stealth_utils import StealthManager
from pathlib import Path


CITY_STATE_MAP = {
    "mumbai": "MAHARASHTRA",
    "delhi": "DELHI",
    "bangalore": "KARNATAKA",
    "hyderabad": "TELANGANA",
    "ahmedabad": "GUJARAT",
    "chennai": "TAMIL NADU",
    "kolkata": "WEST BENGAL",
    "surat": "GUJARAT",
    "pune": "MAHARASHTRA",
    "jaipur": "RAJASTHAN",
    "lucknow": "UTTAR PRADESH",
    "kanpur": "UTTAR PRADESH",
    "nagpur": "MAHARASHTRA",
    "indore": "MADHYA PRADESH",
    "thane": "MAHARASHTRA",
    "bhopal": "MADHYA PRADESH",
    "visakhapatnam": "ANDHRA PRADESH",
    "pimpri-chinchwad": "MAHARASHTRA",
    "patna": "BIHAR",
    "vadodara": "GUJARAT",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    from processing import ProcessingHandler
    import scrapers # This triggers registration of all scrapers via scrapers/__init__.py
    from scrapers.base import ScraperRegistry
    
    # Legacy fallbacks for naming consistency in older code
    # These are only needed if some parts of the code still reference them directly
    # But since we use ScraperRegistry, we can just define them as None or their actual class if needed
    from scrapers.official import NSEScraper, BSEScraper
    NSEBrokerScraper = NSEScraper
    BSEBrokerScraper = BSEScraper
    
except ImportError as e:
    logger.warning(f"Failed to import/register modular scrapers: {e}")

PROJ_DIR = Path(__file__).parent
EXPORTS_DIR = PROJ_DIR / "exports"
LOGS_DIR = PROJ_DIR / "logs"
EXPORTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

OFFICIAL_CATEGORY_SOURCE_MAP = {
    # Financial Services (Regulated)
    "mutual-fund-agents": ["AMFI"],
    "mutual-fund-agent": ["AMFI"],
    "mutual-fund-advisor": ["AMFI"],
    "insurance-agents": ["IRDAI"],
    "insurance-agent": ["IRDAI"],
    "insurance-advisor": ["IRDAI"],
    "insurance-consultant": ["IRDAI"],
    "tax-advocates": ["ICAI"],
    "tax-advocate": ["ICAI"],
    "tax-consultant": ["ICAI"],
    "chartered-accountants": ["ICAI"],
    "chartered-accountant": ["ICAI"],
    "ca": ["ICAI"],
    "company-secretaries": ["ICSI"],
    "secretaries": ["ICSI"],
    "stock-brokers": ["NSE", "BSE"],
    "stock-broker": ["NSE", "BSE"],
    "sebi-registered": ["SEBI"],
    "sebi-advisor": ["SEBI"],
    "investment-advisor": ["SEBI"],
    "investment-advisors": ["SEBI"],
    "investment-adviser": ["SEBI"],
    "investment-advisers": ["SEBI"],
    "pms-providers": ["SEBI"],
    "advisor": ["SEBI", "JUSTDIAL", "YELLOWPAGES"],
    "gst-practitioners": ["GST"],
    "insolvency-professionals": ["IBBI"],
    "insolvency-professional": ["IBBI"],
    "registered-valuers": ["IBBI", "YELLOWPAGES"],
    "lawyers": ["BAR_COUNCIL", "YELLOWPAGES", "VYKARI", "SITEMAP"],
    "lawyer": ["BAR_COUNCIL", "YELLOWPAGES", "VYKARI", "SITEMAP"],
    "advocates": ["BAR_COUNCIL", "YELLOWPAGES", "VYKARI", "SITEMAP"],
    "advocate": ["BAR_COUNCIL", "YELLOWPAGES", "VYKARI", "SITEMAP"],
    "gst-consultant": ["GST", "ASKLAILA"],
    "gst": ["GST", "ASKLAILA"],
    "rbi-regulated": ["RBI", "YELLOWPAGES", "SITEMAP"],
    "banks": ["RBI", "YELLOWPAGES", "SITEMAP"],
    "nbfc": ["RBI", "YELLOWPAGES", "SITEMAP"],
    "microfinance": ["RBI", "YELLOWPAGES", "SITEMAP"],
    "financial-advisor": ["AMFI", "SEBI", "YELLOWPAGES", "SITEMAP"],
    "wealth-manager": ["AMFI", "SEBI", "YELLOWPAGES", "SITEMAP"],
    "investment-consultant": ["SEBI", "YELLOWPAGES", "SITEMAP"],
    # Business Directories
    "business-consultants": ["YELLOWPAGES", "TRADEINDIA", "INDIAMART", "EXPORTERSINDIA"],
    "chartered-engineers": ["YELLOWPAGES", "TRADEINDIA", "EXPORTERSINDIA"],
    "cost-accountants": ["YELLOWPAGES", "ASKLAILA"],
    "real-estate-agents": ["YELLOWPAGES", "ASKLAILA", "SITEMAP"],
    "architects": ["YELLOWPAGES", "ASKLAILA", "SITEMAP"],
    "doctors": ["YELLOWPAGES", "ASKLAILA", "SITEMAP"],
    "merchants": ["YELLOWPAGES", "ASKLAILA", "SITEMAP"],
    "exporters": ["EXPORTERSINDIA", "YELLOWPAGES", "SITEMAP"],
    "importers": ["EXPORTERSINDIA", "YELLOWPAGES", "SITEMAP"],
    "startups": ["YELLOWPAGES", "SITEMAP"],
    "business": ["YELLOWPAGES", "TRADEINDIA", "INDIAMART", "JUSTDIAL", "EXPORTERSINDIA", "ASKLAILA", "SITEMAP"],
    "local": ["YELLOWPAGES", "GROTAL", "SULEKHA", "CLICKINDIA", "VYKARI"],
    "person": ["LINKEDIN"],
    "lead": ["LINKEDIN", "GMB", "SITEMAP"],
    "professional": ["LINKEDIN", "SEBI", "NSE", "ICSI", "ICAI", "SITEMAP"],
    "map": ["GMB"],
}


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
    max_pages: int
    dashboard_page_size: int
    categories: List[str]
    cities: List[str]
    scraper_settings: Dict
    max_concurrent: int
    redis_url: Optional[str] = None


def _normalize_proxy_host(host: str, default_port: Optional[str] = None) -> str:
    host = (host or "").strip()
    if not host:
        return ""

    host = re.sub(r"^https?://", "", host, flags=re.IGNORECASE).rstrip("/")
    if ":" not in host:
        port = default_port or ("823" if "dataimpulse.com" in host else "")
        if port:
            host = f"{host}:{port}"
    return host


def load_config() -> Config:
    data = {}
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}

    scraper_cfg = data.get("scraper", {})
    db_cfg = data.get("database", {})
    proxy_cfg = data.get("proxy", {})
    scraper_settings = data.get("scraper_settings", {})

    # Environment Variable Support (Prioritize these for Railway)
    # Environment Variable Support (Prioritize these for Railway)
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        from urllib.parse import urlparse

        # Railway/Heroku sometimes use postgres:// instead of postgresql://
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        try:
            result = urlparse(db_url)
            db_user = result.username
            db_password = result.password
            db_host = result.hostname
            db_port = result.port or 5432
            db_name = result.path.lstrip("/")

            if not all([db_user, db_host, db_name]):
                raise ValueError("Incomplete DATABASE_URL")

        except Exception as e:
            logger.error(
                f"Failed to parse DATABASE_URL: {e}. Falling back to env vars."
            )
            db_host = os.environ.get("DB_HOST", db_cfg.get("host", "localhost"))
            db_port = int(os.environ.get("DB_PORT", db_cfg.get("port", 5432)))
            db_name = os.environ.get("DB_NAME", db_cfg.get("name", "scraper_db"))
            db_user = os.environ.get("DB_USER", db_cfg.get("user", "postgres"))
            db_password = os.environ.get("DB_PASSWORD", db_cfg.get("password", ""))
    else:
        db_host = os.environ.get("DB_HOST", db_cfg.get("host", "localhost"))
        db_port = int(os.environ.get("DB_PORT", db_cfg.get("port", 5432)))
        db_name = os.environ.get("DB_NAME", db_cfg.get("name", "scraper_db"))
        db_user = os.environ.get("DB_USER", db_cfg.get("user", "postgres"))
        db_password = os.environ.get("DB_PASSWORD", db_cfg.get("password", ""))

    proxies = []
    env_proxy_host = os.environ.get("PROXY_HOST")
    if env_proxy_host:
        proxies.append(
            {
                "host": _normalize_proxy_host(
                    env_proxy_host,
                    os.environ.get("PROXY_PORT") or proxy_cfg.get("port"),
                ),
                "username": os.environ.get("PROXY_USER", ""),
                "password": os.environ.get("PROXY_PASS", ""),
            }
        )
    elif "proxies" in proxy_cfg:
        for p in proxy_cfg["proxies"]:
            proxies.append(
                {
                    "host": _normalize_proxy_host(p.get("host", ""), p.get("port")),
                    "username": p.get("username", ""),
                    "password": p.get("password", ""),
                }
            )
    elif proxy_cfg.get("host"):
        proxies.append(
            {
                "host": _normalize_proxy_host(proxy_cfg["host"], proxy_cfg.get("port")),
                "username": proxy_cfg.get("username", ""),
                "password": proxy_cfg.get("password", ""),
            }
        )

    return Config(
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        proxies=proxies,
        request_delay_min=int(
            os.environ.get("SCRAPER_DELAY_MIN", scraper_settings.get("request_delay_min", 5))
        ),
        request_delay_max=int(
            os.environ.get("SCRAPER_DELAY_MAX", scraper_settings.get("request_delay_max", 15))
        ),
        max_retries=int(
            os.environ.get("SCRAPER_MAX_RETRIES", scraper_settings.get("max_retries", 3))
        ),
        timeout_seconds=int(
            os.environ.get("SCRAPER_TIMEOUT", scraper_settings.get("timeout", 60))
        ),
        max_concurrent=int(
            os.environ.get("MAX_CONCURRENT", scraper_settings.get("max_concurrent", 5))
        ),
        headless=os.environ.get(
            "SCRAPER_HEADLESS", str(scraper_settings.get("headless", True))
        ).lower()
        == "true",
        test_mode=os.environ.get(
            "SCRAPER_TEST_MODE", str(scraper_settings.get("test_mode", False))
        ).lower()
        == "true",
        export_csv=os.environ.get(
            "SCRAPER_EXPORT_CSV", str(scraper_settings.get("export_csv", True))
        ).lower()
        == "true",
        csv_output_dir=os.environ.get(
            "SCRAPER_EXPORT_DIR", scraper_settings.get("csv_output_dir", "exports")
        ),
        enable_email_extraction=os.environ.get(
            "SCRAPER_ENABLE_EMAIL",
            str(scraper_settings.get("enable_email_extraction", True)),
        ).lower()
        == "true",
        enable_sitemap=os.environ.get(
            "SCRAPER_ENABLE_SITEMAP", str(scraper_settings.get("enable_sitemap", False))
        ).lower()
        == "true",
        enable_deduplication=os.environ.get(
            "SCRAPER_ENABLE_DEDUPE", str(scraper_settings.get("enable_deduplication", True))
        ).lower()
        == "true",
        enable_email_verify=os.environ.get(
            "SCRAPER_ENABLE_EMAIL_VERIFY",
            str(scraper_settings.get("enable_email_verify", False)),
        ).lower()
        == "true",
        enable_enrichment=os.environ.get(
            "SCRAPER_ENABLE_ENRICH", str(scraper_settings.get("enable_enrichment", False))
        ).lower()
        == "true",
        scheduler_enabled=os.environ.get(
            "SCRAPER_SCHEDULER_ENABLED",
            str(scraper_settings.get("scheduler_enabled", False)),
        ).lower()
        == "true",
        scheduler_interval_hours=int(
            os.environ.get(
                "SCRAPER_SCHEDULER_INTERVAL",
                str(scraper_settings.get("scheduler_interval_hours", 24)),
            )
        ),
        max_pages=int(
            os.environ.get(
                "SCRAPER_MAX_PAGES", scraper_settings.get("max_pages_per_source", 5)
            )
        ),
        dashboard_page_size=scraper_settings.get("dashboard_page_size", 50),
        categories=data.get("categories", []),
        cities=data.get("cities", []),
        scraper_settings=scraper_settings,
        redis_url=os.environ.get("REDIS_URL") or data.get("redis", {}).get("url")
    )


def save_progress(city: str, category: str, source: str, page: int):
    progress_file = LOGS_DIR / "progress.json"
    progress = {}
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
    progress[f"{source}_{category}_{city}"] = {
        "page": page,
        "last_updated": datetime.now().isoformat(),
    }
    progress_file.write_text(json.dumps(progress))


def load_progress(city: str, category: str, source: str) -> int:
    progress_file = LOGS_DIR / "progress.json"
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
        key = f"{source}_{category}_{city}"
        if key in progress:
            return progress[key].get("page", 1)
    return 1


class EmailVerifier:
    EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

    @staticmethod
    def extract_from_text(text: str) -> Optional[str]:
        match = EmailVerifier.EMAIL_REGEX.search(text)
        return match.group(0) if match else None

    @staticmethod
    async def verify_email(email: str) -> bool:
        if not email:
            return False
        domain = email.split("@")[1] if "@" in email else None
        if not domain:
            return False
        valid_domains = [
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "rediffmail.com",
        ]
        if domain.lower() in valid_domains:
            return True
        return True


class DataEnricher:
    @staticmethod
    async def enrich_contact(contact: Dict) -> Dict:
        contact["enriched"] = False
        contact["verified"] = False

        if contact.get("phone"):
            phone = re.sub(r"[^\d]", "", contact["phone"])
            if len(phone) >= 10:
                contact["phone_clean"] = phone[-10:]

        if contact.get("email"):
            contact["email_valid"] = bool(
                EmailVerifier.EMAIL_REGEX.match(contact["email"])
            )

        return contact


class ProxyManager:
    def __init__(self, proxies: List[Dict], test_mode: bool):
        self.proxies = proxies
        self.test_mode = test_mode
        self.current_index = 0

    def get_proxy(self) -> Optional[Dict]:
        # Global Kill-Switch: If SCRAPER_USE_PROXY is explicitly false, never use proxies.
        if os.environ.get("SCRAPER_USE_PROXY", "true").lower() == "false":
            return None
            
        if self.test_mode or not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

    def get_proxy_string(self) -> Optional[str]:
        proxy = self.get_proxy()
        if not proxy:
            return None
        if proxy.get("username") and proxy.get("password"):
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
        self.rate_limiter = RateLimiter(
            config.request_delay_min, config.request_delay_max
        )
        self.browser_proxy_disabled = False
        self.sqlite_conn = None
        self.use_sqlite = False
        
        # 2026 Persistence State
        self.session_ua = None
        self.session_leads_count = 0  

        # Initialize from Global Registry for modularity
        self.scrapers = []
        self.business_scrapers = []
        
        for s in ScraperRegistry.list_scrapers():
            if s.source_name in ["JUSTDIAL", "INDIAMART", "SULEKHA", "YELLOWPAGES", "TRADEINDIA", "EXPORTERSINDIA", "GMB", "LINKEDIN", "FOOTPRINT", "SITEMAP"]:
                self.business_scrapers.append(s)
            else:
                self.scrapers.append(s)

        self.stats = {
            "total_scrape": 0,
            "successful": 0,
            "failed": 0,
            "duplicates_skipped": 0,
            "by_source": {},
        }

    def _normalize_key(self, value: Optional[str]) -> str:
        if not value:
            return ""
        return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")

    def _select_scrapers(
        self, category: str, source_name: Optional[str], use_business: bool
    ) -> List[BaseScraper]:
        if source_name:
            return [
                s
                for s in self.scrapers + self.business_scrapers
                if s.source_name == source_name
            ]

        if use_business:
            return list(self.business_scrapers)

        expected_sources = OFFICIAL_CATEGORY_SOURCE_MAP.get(
            self._normalize_key(category)
        )
        if not expected_sources:
            return list(self.scrapers)

        all_scrapers = self.scrapers + self.business_scrapers
        filtered = [s for s in all_scrapers if s.source_name in expected_sources]
        return filtered or list(self.scrapers)

    def _is_proxy_error(self, error: Exception) -> bool:
        message = str(error).lower()
        return "proxy" in message and (
            "err_proxy_connection_failed" in message
            or "proxy connection failed" in message
            or "proxy authentication" in message
        )

    async def _close_browser(self, stop_playwright: bool = False):
        """DEPRECATED: Browser logic removed for Railway stability. Pure HTTP only."""
        pass

    async def ensure_browser(self):
        if self.page and self.browser and self.context:
            return
        await self.init_browser()

    async def init_db(self):
        try:
            # Try PostgreSQL first
            # Use SSL if a password is set (cloud databases)
            ssl_ctx = "require" if self.config.db_password else None

            self.pool = await asyncpg.create_pool(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                min_size=1,
                max_size=10,
                command_timeout=60,
                ssl=ssl_ctx,
            )
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")

            logger.info("Connected to PostgreSQL successfully")
            await self._create_pg_tables()
            # Unified migration engine runs here
            await self._ensure_column_widths()

        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            # If DATABASE_URL is set, we're in cloud — DO NOT fall back to SQLite
            if os.environ.get("DATABASE_URL"):
                logger.critical("FATAL: Production database unreachable. Aborting.")
                raise e

            self.use_sqlite = True
            import sqlite3

            self.sqlite_conn = sqlite3.connect(PROJ_DIR / "scraper_local.db")
            self._create_sqlite_tables()
            logger.info("SQLite fallback active.")

    async def _ensure_column_widths(self):
        """Proactively expand VARCHAR columns to 500 to prevent data loss in existing tables."""
        logger.info("Running database schema migration check...")
        try:
            async with self.pool.acquire() as conn:
                # Check current widths
                rows = await conn.fetch("""
                    SELECT column_name, character_maximum_length 
                    FROM information_schema.columns 
                    WHERE table_name = 'contacts' AND data_type = 'character varying'
                """)
                
                for row in rows:
                    col = row['column_name']
                    max_len = row['character_maximum_length']
                    
                    # 1. Expand all standard string columns to TEXT
                    if max_len and col not in ['phone', 'phone_clean']:
                        logger.info(f"Migrating column '{col}' to TEXT...")
                        await conn.execute(f"ALTER TABLE contacts ALTER COLUMN {col} TYPE TEXT")
                
                # 2. Forcefully upgrade text-heavy fields to TEXT
                for col in ["name", "email", "address", "category", "city", "area", "state", "source", "source_url", "arn", "license_no", "membership_no", "blockchain_ca"]:
                    try:
                        await conn.execute(f"ALTER TABLE contacts ALTER COLUMN {col} TYPE TEXT")
                        logger.debug(f"Ensured {col} is TEXT")
                    except Exception: pass

                logger.info("Database schema migration complete.")
        except Exception as e:
            logger.error(f"Schema migration failed: {e}")

    async def _create_pg_tables(self):
        # We perform these checks one by one to avoid collision errors in multi-worker environments
        try:
            await self.pool.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    phone VARCHAR(50),
                    email TEXT,
                    address TEXT,
                    category TEXT,
                    city TEXT,
                    area TEXT,
                    state TEXT,
                    source TEXT,
                    source_url TEXT,
                    phone_clean VARCHAR(50),
                    email_valid BOOLEAN,
                    enriched BOOLEAN,
                    arn TEXT,
                    license_no TEXT,
                    membership_no TEXT,
                    quality_score INT DEFAULT 0,
                    quality_tier VARCHAR(20) DEFAULT 'low',
                    blockchain_ca TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            logger.warning(
                f"Table creation skipped or failed (possibly concurrent): {e}"
            )

        required_columns = {
            "name": "TEXT",
            "phone": "VARCHAR(50)",
            "email": "TEXT",
            "address": "TEXT",
            "category": "TEXT",
            "city": "TEXT",
            "area": "TEXT",
            "state": "TEXT",
            "source": "TEXT",
            "source_url": "TEXT",
            "phone_clean": "VARCHAR(50)",
            "email_valid": "BOOLEAN",
            "enriched": "BOOLEAN",
            "arn": "TEXT",
            "license_no": "TEXT",
            "membership_no": "TEXT",
            "quality_score": "INT DEFAULT 0",
            "quality_tier": "VARCHAR(20) DEFAULT 'low'",
            "blockchain_ca": "TEXT",
            "scraped_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for column_name, column_type in required_columns.items():
            try:
                await self.pool.execute(
                    f"ALTER TABLE contacts ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                )
            except Exception as e:
                logger.warning(
                    f"Column migration skipped for contacts.{column_name}: {e}"
                )

        # Individual index creation with error handling for concurrency
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)",
            "CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)",
            "CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean_unique ON contacts(phone_clean) WHERE phone_clean IS NOT NULL",
        ]:
            try:
                await self.pool.execute(idx_sql)
            except Exception as e:
                # If it already exists or there's a lock, we can ignore it as long as the index is there
                logger.debug(f"Index creation notice: {e}")


        try:
            await self.pool.execute(
                "CREATE TABLE IF NOT EXISTS scrape_logs (id SERIAL PRIMARY KEY, source VARCHAR(100), status VARCHAR(50), records_count INTEGER, error_message TEXT, started_at TIMESTAMP, completed_at TIMESTAMP)"
            )
        except Exception:
            pass

    def _create_sqlite_tables(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                category TEXT,
                city TEXT,
                area TEXT,
                state TEXT,
                source TEXT,
                source_url TEXT,
                phone_clean TEXT,
                email_valid BOOLEAN,
                enriched BOOLEAN,
                arn TEXT,
                license_no TEXT,
                membership_no TEXT,
                quality_score INTEGER DEFAULT 0,
                quality_tier TEXT DEFAULT 'low',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean_unique ON contacts(phone_clean) WHERE phone_clean IS NOT NULL")
        self.sqlite_conn.commit()

    async def init_browser(
        self, disable_proxy: bool = False, force_restart: bool = False
    ):
        """DEPRECATED: Browser logic removed for Railway stability. Using PoliteHTTPScraper."""
        pass

    async def extract_email_from_detail(self, detail_url: str) -> Optional[str]:
        if not detail_url or not self.config.enable_email_extraction:
            return None

        try:
            await self.page.goto(
                detail_url,
                timeout=self.config.timeout_seconds * 1000,
                wait_until="networkidle",
            )
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

        if hasattr(self, "use_sqlite") and self.use_sqlite:
            cursor = self.sqlite_conn.cursor()
            if phone:
                cursor.execute(
                    "SELECT 1 FROM contacts WHERE phone_clean = ? LIMIT 1", (phone,)
                )
                if cursor.fetchone():
                    return True
            if email:
                cursor.execute(
                    "SELECT 1 FROM contacts WHERE email = ? LIMIT 1", (email,)
                )
                if cursor.fetchone():
                    return True
            return False
        
        # PostgreSQL support
        if self.pool:
            async with self.pool.acquire() as conn:
                if phone:
                    row = await conn.fetchrow(
                        "SELECT 1 FROM contacts WHERE phone_clean = $1 LIMIT 1", phone
                    )
                    if row:
                        return True
                if email:
                    row = await conn.fetchrow(
                        "SELECT 1 FROM contacts WHERE email = $1 LIMIT 1", email
                    )
                    if row:
                        return True
        return False

    async def _filter_duplicates_bulk(self, listings: List[Dict]) -> List[Dict]:
        if not self.config.enable_deduplication or not listings:
            return listings

        phones = {l["phone_clean"] for l in listings if l.get("phone_clean")}
        emails = {l["email"] for l in listings if l.get("email")}

        existing_phones = set()
        existing_emails = set()

        try:
            if hasattr(self, "use_sqlite") and self.use_sqlite:
                cursor = self.sqlite_conn.cursor()
                if phones:
                    placeholders = ",".join(["?"] * len(phones))
                    cursor.execute(
                        f"SELECT phone_clean FROM contacts WHERE phone_clean IN ({placeholders})",
                        list(phones),
                    )
                    existing_phones = {r[0] for r in cursor.fetchall()}
                if emails:
                    placeholders = ",".join(["?"] * len(emails))
                    cursor.execute(
                        f"SELECT email FROM contacts WHERE email IN ({placeholders})",
                        list(emails),
                    )
                    existing_emails = {r[0] for r in cursor.fetchall()}
            else:
                async with self.pool.acquire() as conn:
                    if phones:
                        phone_list = list(phones)
                        for i in range(0, len(phone_list), 500):
                            chunk = phone_list[i : i + 500]
                            rows = await conn.fetch(
                                "SELECT phone_clean FROM contacts WHERE phone_clean = ANY($1)",
                                chunk,
                            )
                            existing_phones.update({r["phone_clean"] for r in rows})
                    if emails:
                        email_list = list(emails)
                        for i in range(0, len(email_list), 500):
                            chunk = email_list[i : i + 500]
                            rows = await conn.fetch(
                                "SELECT email FROM contacts WHERE email = ANY($1)",
                                chunk,
                            )
                            existing_emails.update({r["email"] for r in rows})
        except Exception as e:
            logger.warning(f"Error in bulk deduplication: {e}")
            return listings

        return [
            l
            for l in listings
            if (not l.get("phone_clean") or l["phone_clean"] not in existing_phones)
            and (not l.get("email") or l["email"] not in existing_emails)
        ]

    async def _process_listings(self, listings: List[Dict]) -> List[Dict]:
        """Process and clean listings using the unified ProcessingHandler."""
        # Filter duplicates in bulk first to avoid thousands of SQL queries
        listings = await self._filter_duplicates_bulk(listings)

        processed_listings = []

        for listing in listings:
            if (
                self.config.enable_email_extraction
                and listing.get("detail_url")
                and not listing.get("email")
            ):
                email = await self.extract_email_from_detail(listing["detail_url"])
                listing["email"] = email

            # Use Unified Processing Handler
            listing = ProcessingHandler.process_contact(listing)
            
            # Discard if the processing handler returned None (e.g. no phone/email)
            if listing is None:
                continue

            # Final safety check against DB (especially if cleaning changed the phone)
            is_dup = await self.is_duplicate(
                listing.get("phone_clean"), listing.get("email")
            )
            if is_dup:
                self.stats["duplicates_skipped"] += 1
                continue

            processed_listings.append(listing)
            
        # Quality logs
        total_attempted = len(listings)
        total_saved = len(processed_listings)
        skipped_junk = total_attempted - total_saved - self.stats.get("duplicates_skipped", 0)
        
        if skipped_junk > 0:
            logger.info(f"[QUALITY] Filter: Discarded {skipped_junk} contacts missing required phone/email info.")
            self.stats["skipped_junk"] = self.stats.get("skipped_junk", 0) + skipped_junk

        return processed_listings

    def _format_amfi_listing(self, record: Dict, city: str) -> Dict:
        if not record or not isinstance(record, dict):
            return {}

        phone = record.get("TelephoneNumber_O") or record.get("TelephoneNumber_R")
        record_city = record.get("City") or city
        normalized_city = record_city.title() if isinstance(record_city, str) else city

        return {
            "name": (record.get("ARNHolderName") or "").strip(),
            "phone": phone.strip()
            if isinstance(phone, str) and phone.strip()
            else None,
            "email": (record.get("Email") or "").strip() or None,
            "address": (record.get("Address") or "").strip(' "'),
            "area": None,
            "state": CITY_STATE_MAP.get(
                self._normalize_key(normalized_city),
                CITY_STATE_MAP.get(self._normalize_key(city)),
            ),
            "city": normalized_city,
            "arn": (record.get("ARN") or "").strip() or None,
            "detail_url": None,
        }

    async def scrape_amfi_api(
        self, scraper: Any, city: str, category: str, on_progress=None
    ) -> List[Dict]:
        all_listings = []
        total_leads_acc = 0
        page_num = 1
        page_size = int(os.environ.get("SCRAPER_AMFI_PAGE_SIZE", "100"))
        timeout = aiohttp.ClientTimeout(total=max(30, self.config.timeout_seconds))

        user_agent = StealthManager.get_random_ua()
        headers = StealthManager.get_modern_headers(user_agent)
        use_proxy_for_fast = os.environ.get("SCRAPER_USE_PROXY", "false").lower() == "true"
        proxy = self.proxy_manager.get_proxy_string() if use_proxy_for_fast else None
        if proxy:
            logger.info("Fast extraction proxy enabled by SCRAPER_USE_PROXY.")
        else:
            logger.info("Fast extraction using direct HTTP with polite backoff.")

        # Disable trust_env if proxy is used to prevent 127.0.0.1 loops
        async with aiohttp.ClientSession(
            timeout=timeout, 
            headers=headers,
            trust_env=False if proxy else True
        ) as session:
            while True:
                params = {
                    "strOpt": "ALL",
                    "city": city.upper(),
                    "search": "",
                    "page": page_num,
                    "pageSize": page_size
                }
                logger.info(f"Fetching AMFI API page {page_num} for {city}")

                try:
                    async with session.get(
                        scraper.SEARCH_API_URL, params=params, proxy=proxy
                    ) as response:
                        if response.status != 200:
                            logger.error(f"AMFI API error {response.status}")
                            break

                        payload = await response.json(content_type=None)
                except Exception as e:
                    logger.error(f"AMFI API connection failed: {e}")
                    break

                if not payload or not isinstance(payload, (dict, list)):
                    break

                # Support both old 'data' and new 'list' schemas
                records = []
                if isinstance(payload, dict):
                    records = payload.get("list") or payload.get("data") or []
                elif isinstance(payload, list):
                    records = payload

                if not records:
                    break

                batch = []
                for record in records:
                    # Map new 2026 fields to internal schema
                    leads_data = {
                        "name": record.get("ARNHolderName") or record.get("name") or record.get("distributor_name"),
                        "phone": record.get("TelephoneNumber_O") or record.get("mobile_number") or record.get("phone"),
                        "email": record.get("Email") or record.get("email"),
                        "address": record.get("Address") or record.get("address"),
                        "city": record.get("City") or city,
                        "arn": record.get("ARN") or record.get("arn_number") or record.get("arn")
                    }
                    if leads_data["name"]:
                        batch.append(leads_data)

                batch = await self._process_listings(batch)

                if batch:
                    await self.save_to_db(
                        batch,
                        category,
                        city,
                        scraper.source_name,
                        scraper.SEARCH_API_URL,
                    )
                    all_listings.extend(batch)
                    total_leads_acc += len(batch)
                    
                    # For massive batches, clear processed data from memory early
                    if len(all_listings) > 5000:
                        all_listings = []  # We've already saved it, keep memory low

                meta = payload.get("meta") or {}
                total_pages = meta.get("pageCount") or page_num
                if page_num == 1:
                    logger.info(
                        f"AMFI API total for {city}: {meta.get('total', len(records))} records across {total_pages} page(s) with pageSize={page_size}"
                    )
                if on_progress:
                    on_progress(
                        {
                            "page": page_num,
                            "total_pages": total_pages,
                            "leads": len(batch),
                            "source": scraper.source_name,
                        }
                    )

                if page_num >= total_pages:
                    break

                page_num += 1
                
                # Anti-Detection: Randomized delay between page requests (Phase 2)
                delay = random.uniform(
                    self.config.request_delay_min, self.config.request_delay_max
                )
                logger.info(f"Stealth jitter: Sleeping for {delay:.2f}s before next page")
                await asyncio.sleep(delay)
                
                await self.rate_limiter.wait()

        logger.info(f"AMFI API extracted {total_leads_acc} listings for {city}")
        self.rate_limiter.record_success()
        self.stats["successful"] += 1
        # Return a list of size total_leads_acc (dummy objects if memory cleared) 
        # so that callers can accurately count without holding 100k objects in RAM
        return [None] * total_leads_acc

    async def scrape_category_fast(self, city: str, category: str, source_name: Optional[str] = None):
        """
        High-speed extraction bypassing the browser.
        Targets official registries and open APIs.
        Optimized for Railway 512MB RAM tiers.
        """
        from polite_http_scraper import PoliteHTTPScraper
        from api_handlers import OfficialAPIHandlers
        
        # Determine the target source
        normalized_category = self._normalize_key(category)
        if source_name is None and normalized_category not in OFFICIAL_CATEGORY_SOURCE_MAP:
            fallback_sources = {"YELLOWPAGES", "ASKLAILA", "SITEMAP"}
            sources = [
                s
                for s in self.scrapers + self.business_scrapers
                if s.source_name in fallback_sources
            ]
            logger.info(
                "No fast source map for %s; using directory fallbacks only.",
                category,
            )
        else:
            sources = self._select_scrapers(category, source_name, use_business=False)

        total_extracted = 0
        
        proxy = self.proxy_manager.get_proxy_string()
        async with PoliteHTTPScraper(max_concurrent=self.config.max_concurrent, proxy=proxy) as fast_engine:
            for scraper_obj in sources:
                source = scraper_obj.source_name
                logger.info(f"⚡ Fast Extraction: {source} | {category} | {city}")

                if not OfficialAPIHandlers.supports(source, category):
                    logger.info(
                        "Skipping %s for %s/%s: no fast HTTP handler.",
                        source,
                        city,
                        category,
                    )
                    continue
                
                try:
                    # 1. Specialized API Handlers (High-Speed)
                    batch = await OfficialAPIHandlers.dispatch(
                        source, fast_engine, city, category
                    )
                    
                    # 2. Legacy API Fallback (AMFI)
                    if not batch and source == "AMFI":
                        # scrape_amfi_api handles its own internal saving to optimize memory
                        batch = await self.scrape_amfi_api(scraper_obj, city, category)
                        total_extracted += len(batch)
                        logger.info(f"✅ Extracted {len(batch)} from {source}")
                        continue
                    
                    # 3. Save to DB for other handlers
                    if batch:
                        await self.save_to_db(batch, category, city, source, "Official API")
                        total_extracted += len(batch)
                        logger.info(f"✅ Extracted {len(batch)} from {source}")
                    else:
                        logger.warning(f"No results for {source} in {city} via fast engine.")
                        
                except Exception as e:
                    if "PROXY_TRAFFIC_EXHAUSTED" in str(e):
                        raise
                    logger.error(f"Fast extraction failed for {source}: {e}")
                    
        return total_extracted

    async def scrape_page(
        self,
        url: str,
        city: str = None,
        category: str = None,
        scraper: Optional[BaseScraper] = None,
        max_pages: Optional[int] = None,
        on_progress=None,
    ) -> List[Dict]:
        all_listings = []
        limit = max_pages or self.config.max_pages
        start_page = load_progress(
            city, category, scraper.source_name if scraper else "Unknown"
        )

        if start_page > limit:
            logger.info(
                f"Already scraped {start_page - 1} pages, which meets or exceeds limit {limit}. Resetting to page 1."
            )
            start_page = 1

        for page_num in range(start_page, limit + 1):
            page_url = (
                scraper.build_search_url(city, category, page_num)
                if scraper and page_num > 1
                else url
            )
            if page_num > 1 and page_url == url:
                break

            retries = 0
            success = False

            while retries < self.config.max_retries and not success:
                try:
                    logger.info(f"Fetching: {page_url}")

                    await self.ensure_browser()
                    await self.page.goto(
                        page_url,
                        timeout=self.config.timeout_seconds * 1000,
                        wait_until="networkidle",
                    )
                    await asyncio.sleep(2)

                    page_title = await self.page.title()
                    page_url_final = self.page.url
                    logger.info(
                        f"Page loaded - Title: {page_title}, URL: {page_url_final}"
                    )

                    # 1. Human-like interaction (vCPU optimized)
                    await self.human_scroll(self.page)

                    page_text = await self.page.inner_text("body")
                    page_text_lower = page_text.lower()

                    if (
                        "captcha" in page_text_lower
                        or "verify" in page_text_lower
                        or "robot" in page_text_lower
                        or "unusual activity" in page_text_lower
                    ):
                        logger.warning("[DEBUG] Detection: CAPTCHA or bot detection detected!")
                        # Rotate proxy profile on next retry
                        self.proxy_manager.get_proxy() 
                        break

                    error_signatures = [
                        "service unavailable",
                        "gateway timeout",
                        "404 not found",
                        "cannot find the requested page",
                        "azure front door",
                    ]
                    if any(
                        signature in page_text_lower for signature in error_signatures
                    ):
                        logger.warning(
                            "Source returned an error page; skipping extraction for this source"
                        )
                        break

                    await self.rate_limiter.wait()

                    # THE GOLDEN RULE: Save raw HTML first before parsing (0:37)
                    html_content = await self.page.content()
                    raw_path = storage.save(
                        html_content,
                        scraper.source_name if scraper else "Unknown",
                        city,
                        category,
                    )
                    if raw_path:
                        logger.info(f"[STORAGE] Raw HTML saved: {raw_path}")

                    # Anti-Detection: Standard randomized jitter delay (Updated to 5-15s)
                    jitter = random.uniform(
                        self.config.request_delay_min, self.config.request_delay_max
                    )
                    logger.info(f"Stealth jitter: Sleeping for {jitter:.2f}s")
                    await asyncio.sleep(jitter)

                    # Extract data from current page (Optionally using raw HTML)
                    listings = await self._extract_current_page(
                        city, category, scraper, html_content=html_content
                    )
                    logger.info(
                        f"Extracted {len(listings)} listings from page {page_num}"
                    )

                    if not listings:
                        logger.warning(f"No listings found on page {page_num}")
                        break

                    processed = await self._process_listings(listings)
                    if processed:
                        await self.save_to_db(
                            processed,
                            category,
                            city,
                            scraper.source_name if scraper else "Unknown",
                            self.page.url,
                        )
                        all_listings.extend(processed)

                        if on_progress:
                            on_progress(
                                {
                                    "page": page_num,
                                    "total_pages": limit,
                                    "leads": len(processed),
                                    "source": scraper.source_name
                                    if scraper
                                    else "Unknown",
                                }
                            )

                        # Clear memory for long crawls
                        if len(all_listings) > 2000:
                            all_listings = []

                    # Update progress after each successful page
                    save_progress(
                        city,
                        category,
                        scraper.source_name if scraper else "Unknown",
                        page_num + 1,
                    )

                    success = True
                    self.rate_limiter.record_success()
                    self.stats["successful"] += 1

                except Exception as e:
                    if self._is_proxy_error(e) and not self.browser_proxy_disabled:
                        logger.warning(
                            "Proxy failed during page fetch, retrying browser without proxy"
                        )
                        await self.init_browser(disable_proxy=True, force_restart=True)
                        continue

                    retries += 1
                    self.rate_limiter.record_failure()
                    self.stats["failed"] += 1
                    logger.warning(f"Retry {retries}/{self.config.max_retries}: {e}")
                    await asyncio.sleep(random.uniform(3, 8))

            if not success:
                logger.error(f"Failed after {self.config.max_retries} retries")

        return all_listings

    async def human_scroll(self, page: Page):
        """Perform human-like scrolling using mouse.wheel to trigger lazy loading and evade detection."""
        try:
            # 1. Randomized scroll steps using physical mouse wheel simulation
            for _ in range(random.randint(3, 7)):
                # Simulate a human finger flick on the scroll wheel/trackpad
                scroll_delta = random.randint(400, 700)
                await page.mouse.wheel(0, scroll_delta)
                
                # Randomized pause to 'read' the middle area
                await asyncio.sleep(random.uniform(0.8, 1.8))
            
            # 2. Scroll to bottom sometimes (more natural)
            if random.random() > 0.6:
                await page.mouse.wheel(0, 2000)
                await asyncio.sleep(random.uniform(1.0, 2.0))
            
            # 3. Scroll back up slightly to simulate re-reading
            await page.mouse.wheel(0, -300)
            await asyncio.sleep(random.uniform(0.5, 1.2))
        except Exception as e:
            logger.debug(f"Human scrolling simulation error: {e}")

    async def _extract_current_page(
        self,
        city: str = None,
        category: str = None,
        scraper: Optional[BaseScraper] = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            if not scraper:
                url_lower = self.page.url.lower()
                if "justdial" in url_lower:
                    scraper = ScraperRegistry.get("JUSTDIAL")
                elif "indiamart" in url_lower:
                    scraper = ScraperRegistry.get("INDIAMART")
                elif "amfi" in url_lower:
                    scraper = ScraperRegistry.get("AMFI")
                elif "irdai" in url_lower or "policyholder" in url_lower:
                    scraper = ScraperRegistry.get("IRDAI")
                elif "icai" in url_lower:
                    scraper = ScraperRegistry.get("ICAI")
                elif "sulekha" in url_lower:
                    scraper = ScraperRegistry.get("SULEKHA")
                elif "clickindia" in url_lower:
                    scraper = ScraperRegistry.get("CLICKINDIA")
                else:
                    scraper = ScraperRegistry.get("YELLOWPAGES")

            listings = await scraper.extract_listings(self.page, city, category)
            
            # Phase 4: Regex Extraction Fallback if DOM selectors fail
            if not listings and html_content:
                logger.info(f"[FALLBACK] No listings found with CSS selectors. Attempting Raw Regex extraction for {scraper.source_name}")
                listings = scraper.extract_raw_fallback(html_content, city, category)
        except Exception as e:
            logger.warning(f"Extraction error: {e}")
            
            # Additional fallback on error
            if html_content and scraper:
                 listings = scraper.extract_raw_fallback(html_content, city, category)
        return listings

    OFFICIAL_REGISTRY_SOURCES = {
        "AMFI",
        "IRDAI",
        "ICAI",
        "ICSI",
        "SEBI",
        "IBBI",
        "NSE",
        "BSE",
        "GST",
        "RBI",
        "MCA",
        "BAR_COUNCIL",
    }

    def _is_official_registry_record(self, listing: Dict, source: str) -> bool:
        source_name = (source or listing.get("source") or "").upper()
        if source_name not in self.OFFICIAL_REGISTRY_SOURCES:
            return False
        name = str(listing.get("name") or "").strip()
        if len(name) < 3:
            return False
        return bool(
            listing.get("phone_clean")
            or (listing.get("email") and listing.get("email_valid"))
            or listing.get("arn")
            or listing.get("license_no")
            or listing.get("membership_no")
            or listing.get("address")
        )

    def _storage_dedupe_key(self, listing: Dict, source: str):
        if listing.get("phone_clean"):
            return ("phone", listing["phone_clean"])
        if listing.get("email"):
            return ("email", str(listing["email"]).lower())
        for field in ("arn", "license_no", "membership_no"):
            value = listing.get(field)
            if value:
                return (field, (source or listing.get("source") or "").upper(), str(value).strip().upper())
        if self._is_official_registry_record(listing, source):
            name = self._normalize_key(listing.get("name"))
            address = self._normalize_key(listing.get("address"))
            if name and address:
                return ("registry", (source or listing.get("source") or "").upper(), name, address)
        return None

    def _registry_exists_sqlite(self, cursor, listing: Dict, source: str) -> bool:
        clauses = []
        params = [source]
        for field in ("arn", "license_no", "membership_no"):
            value = listing.get(field)
            if value:
                clauses.append(f"{field} = ?")
                params.append(value)
        if listing.get("name") and listing.get("address"):
            clauses.append("(LOWER(name) = LOWER(?) AND LOWER(address) = LOWER(?))")
            params.extend([listing.get("name"), listing.get("address")])
        if not clauses:
            return False
        cursor.execute(
            f"SELECT id FROM contacts WHERE source = ? AND ({' OR '.join(clauses)}) LIMIT 1",
            params,
        )
        return cursor.fetchone() is not None

    async def _registry_exists_pg(self, conn, listing: Dict, source: str) -> bool:
        clauses = []
        params = [source]
        idx = 2
        for field in ("arn", "license_no", "membership_no"):
            value = listing.get(field)
            if value:
                clauses.append(f"{field} = ${idx}")
                params.append(value)
                idx += 1
        if listing.get("name") and listing.get("address"):
            clauses.append(f"(LOWER(name) = LOWER(${idx}) AND LOWER(address) = LOWER(${idx + 1}))")
            params.extend([listing.get("name"), listing.get("address")])
        if not clauses:
            return False
        row = await conn.fetchval(
            f"SELECT id FROM contacts WHERE source = $1 AND ({' OR '.join(clauses)}) LIMIT 1",
            *params,
        )
        return row is not None

    async def save_to_db(
        self, listings: List[Dict], category: str, city: str, source: str, url: str
    ):
        if not listings:
            return 0

        # Normalize category to prevent duplicates like "Mutual Fund Agent" vs "mutual fund agent"
        category = category.strip().title() if category else "General"

        prepared = []
        for raw_listing in listings:
            if not raw_listing:
                continue
            listing = dict(raw_listing)
            if listing.get("registration_no") and not listing.get("license_no"):
                listing["license_no"] = listing.get("registration_no")
            listing.setdefault("source", source)
            listing.setdefault("category", category)
            if city:
                listing.setdefault("city", city)
            processed = ProcessingHandler.process_contact(listing)
            if not processed:
                continue
            
            # REQUIREMENT: Only save if we have either phone or email contact info
            has_phone = bool(processed.get("phone") or processed.get("phone_clean"))
            has_email = bool(processed.get("email"))
            if not (has_phone or has_email):
                logger.debug(f"[FILTER] Dropping {processed.get('name')} - No contact info found")
                continue

            if ProcessingHandler.filter_valid([processed]) or self._is_official_registry_record(processed, source):
                prepared.append(processed)

        seen = set()
        valid_listings = []
        for listing in prepared:
            key = self._storage_dedupe_key(listing, source)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            valid_listings.append(listing)

        skipped = len(listings) - len(valid_listings)
        if skipped > 0:
            logger.info(
                f"[STORAGE] Skipped {skipped} invalid or duplicate listings during DB save"
            )

        if not valid_listings:
            return 0

        valid_listings = await self._filter_duplicates_bulk(valid_listings)
        if not valid_listings:
            return 0

        if hasattr(self, "use_sqlite") and self.use_sqlite:
            cursor = self.sqlite_conn.cursor()
            inserted = 0
            for l in valid_listings:
                if (
                    not l.get("phone_clean")
                    and not l.get("email")
                    and self._registry_exists_sqlite(cursor, l, source)
                ):
                    continue
                cursor.execute(
                    """
                    INSERT INTO contacts (
                        name, phone, email, address, category, city, area, state, 
                        source, source_url, phone_clean, email_valid, enriched, 
                        arn, license_no, membership_no, quality_score, quality_tier, scraped_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING
                """,
                    (
                        l.get("name"),
                        l.get("phone"),
                        l.get("email"),
                        l.get("address"),
                        l.get("category") or category,
                        l.get("city") or city,
                        l.get("area"),
                        l.get("state"),
                        l.get("source") or source,
                        l.get("source_url") or url,
                        l.get("phone_clean"),
                        l.get("email_valid", False),
                        l.get("enriched", False),
                        l.get("arn"),
                        l.get("license_no"),
                        l.get("membership_no"),
                        l.get("quality_score", 0),
                        l.get("quality_tier", "low"),
                    ),
                )
                if cursor.rowcount > 0:
                    inserted += 1
            self.sqlite_conn.commit()
            logger.info(f"Saved {inserted} records to SQLite")
            return inserted

        inserted = 0
        async with self.pool.acquire() as conn:
            chunk_size = 25
            for i in range(0, len(valid_listings), chunk_size):
                chunk = valid_listings[i : i + chunk_size]
                try:
                    async with conn.transaction():
                        for l in chunk:
                            if not l.get("phone_clean") and not l.get("email") and await self._registry_exists_pg(conn, l, source): continue
                            st = await conn.execute("""
                                INSERT INTO contacts (name, phone, email, address, category, city, area, state, source, source_url, phone_clean, email_valid, enriched, arn, license_no, membership_no, quality_score, quality_tier)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) ON CONFLICT DO NOTHING
                            """, l.get("name"), l.get("phone"), l.get("email"), l.get("address"), l.get("category") or category, l.get("city") or city, l.get("area"), l.get("state"), l.get("source") or source, l.get("source_url") or url, l.get("phone_clean"), l.get("email_valid", False), l.get("enriched", False), l.get("arn"), l.get("license_no"), l.get("membership_no"), l.get("quality_score", 0), l.get("quality_tier", "low"))
                            if st.endswith(" 1"): inserted += 1
                except Exception as e:
                    logger.warning(f"Sub-batch save failed ({e}), falling back to individual inserts")
                    for l in chunk:
                        try:
                            st = await conn.execute("""
                                INSERT INTO contacts (name, phone, email, address, category, city, area, state, source, source_url, phone_clean, email_valid, enriched, arn, license_no, membership_no, quality_score, quality_tier)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) ON CONFLICT DO NOTHING
                            """, l.get("name"), l.get("phone"), l.get("email"), l.get("address"), l.get("category") or category, l.get("city") or city, l.get("area"), l.get("state"), l.get("source") or source, l.get("source_url") or url, l.get("phone_clean"), l.get("email_valid", False), l.get("enriched", False), l.get("arn"), l.get("license_no"), l.get("membership_no"), l.get("quality_score", 0), l.get("quality_tier", "low"))
                            if st.endswith(" 1"): inserted += 1
                        except Exception as rec_err: 
                            logger.debug(f"Record-level save failure: {rec_err}")

        logger.info(f"Saved {inserted} records to database")
        return inserted

    async def save_contacts(self, contacts: List[Dict]) -> int:
        """Compatibility wrapper for direct scraper tasks."""
        grouped = {}
        for contact in contacts:
            if not contact:
                continue
            source = (contact.get("source") or "DIRECT").upper()
            category = contact.get("category") or "General"
            city = contact.get("city") or "Multiple"
            grouped.setdefault((source, category, city), []).append(contact)

        total_saved = 0
        for (source, category, city), batch in grouped.items():
            total_saved += await self.save_to_db(batch, category, city, source, "Direct Gov")
        return total_saved

    async def export_to_csv(self, source: Optional[str] = None):
        os.makedirs(self.config.csv_output_dir, exist_ok=True)

        if hasattr(self, "use_sqlite") and self.use_sqlite:
            cursor = self.sqlite_conn.cursor()
            if source:
                cursor.execute(
                    "SELECT * FROM contacts WHERE source = ? ORDER BY scraped_at DESC",
                    (source,),
                )
            else:
                cursor.execute("SELECT * FROM contacts ORDER BY scraped_at DESC")
            rows = [dict(r) for r in cursor.fetchall()]
        else:
            async with self.pool.acquire() as conn:
                if source:
                    rows = await conn.fetch(
                        "SELECT * FROM contacts WHERE source = $1 ORDER BY scraped_at DESC",
                        source,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM contacts ORDER BY scraped_at DESC"
                    )
                rows = [dict(r) for r in rows]

            if not rows:
                logger.warning("No data to export")
                return

            filename = f"{self.config.csv_output_dir}/contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))

            logger.info(f"Exported {len(rows)} records to {filename}")
            return filename

    async def get_stats(self) -> Dict:
        if hasattr(self, "use_sqlite") and self.use_sqlite:
            cursor = self.sqlite_conn.cursor()
            total = cursor.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
            with_email = cursor.execute(
                "SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL"
            ).fetchone()[0]
            return {
                "total_contacts": total,
                "with_email": with_email,
                "by_source": {},
                "by_category": {},
            }

        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM contacts")
            by_source = await conn.fetch("""
                SELECT source, COUNT(*) as count FROM contacts GROUP BY source
            """)
            by_category = await conn.fetch("""
                SELECT category, COUNT(*) as count FROM contacts GROUP BY category
            """)
            with_email = await conn.fetchval(
                "SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL"
            )

            return {
                "total_contacts": total,
                "with_email": with_email,
                "by_source": {r["source"]: r["count"] for r in by_source},
                "by_category": {r["category"]: r["count"] for r in by_category},
            }

    async def scrape_category(
        self,
        city: str,
        category: str,
        source_name: Optional[str] = None,
        use_business: bool = False,
        on_progress=None,
    ):
        logger.info(f"\n>>> Scraping: {category} in {city}")

        scrapers_to_run = self._select_scrapers(category, source_name, use_business)

        for scraper in scrapers_to_run:
            url = scraper.build_search_url(city, category)
            logger.info(f"Source: {scraper.source_name}, URL: {url}")

            self.stats["total_scrape"] += 1
            try:
                if scraper.source_name == "AMFI":
                    listings = await self.scrape_amfi_api(
                        scraper, city, category, on_progress=on_progress
                    )
                else:
                    listings = await self.scrape_page(
                        url, city, category, scraper=scraper, on_progress=on_progress
                    )
            except Exception as exc:
                self.stats["failed"] += 1
                logger.error(
                    f"{scraper.source_name} failed for {category} in {city}: {exc}"
                )
                continue

            logger.info(
                f"DEBUG: Processed {len(listings)} listings from {scraper.source_name}"
            )

            # Note: save_to_db is now called internally within scrape_page/scrape_amfi_api for chunked persistence

            self.stats["by_source"][scraper.source_name] = self.stats["by_source"].get(
                scraper.source_name, 0
            ) + len(listings)

            save_progress(
                city, category, scraper.source_name, 1
            )  # Reset for next cycle
            await self.rate_limiter.wait()

    async def run(self):
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("Starting Contact Scraper - Enhanced Version")
        logger.info("=" * 60)

        await self.init_db()

        try:
            for city in self.config.cities:
                for category in self.config.categories:
                    await self.scrape_category(city, category)

            if self.config.export_csv:
                await self.export_to_csv()

            stats = await self.get_stats()
            elapsed = datetime.now() - start_time
            logger.info("\n" + "=" * 60)
            logger.info("SCRAPING COMPLETE")
            logger.info(f"Total contacts: {stats['total_contacts']}")
            logger.info(f"With email: {stats['with_email']}")
            logger.info(f"By source: {stats['by_source']}")
            logger.info(f"By category: {stats['by_category']}")
            logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
            logger.info(f"Time elapsed: {elapsed}")
            logger.info("=" * 60)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
        finally:
            await self.close()

    async def close(self):
        await self._close_browser(stop_playwright=True)
        if self.pool:
            await self.pool.close()
            self.pool = None
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None


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


if __name__ == "__main__":
    asyncio.run(main())


# Modular registration complete via 'import scrapers' above.
logger.info(f"Scraper Engine Ready: {len(ScraperRegistry.list_scrapers())} sources registered.")
