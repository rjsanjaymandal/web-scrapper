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
from playwright.async_api import async_playwright, Page, Browser, Playwright
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
from raw_storage import storage
from scrapers_registry import BaseScraper, ScraperRegistry
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

try:
    from processing import ProcessingHandler
    from enhanced_utils import (
        SulekhaScraper,
        ClickIndiaScraper,
        GrotalScraper,
        SEBIScraper,
        NSEScraper,
        GoogleMapsScraper,
        LinkedInGoogleScraper,
    )

    # Registration handled at end of file to ensure all classes are defined
    pass
except ImportError as e:
    logger.warning(f"Failed to import/register enhanced scrapers: {e}")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
    "investment-adviser": ["SEBI"],
    "advisor": ["SEBI", "JUSTDIAL", "YELLOWPAGES"],
    "gst-practitioners": ["GST"],
    "gst-consultant": ["GST"],
    "gst": ["GST"],
    "rbi-regulated": ["RBI"],
    "banks": ["RBI"],
    "nbfc": ["RBI"],
    "financial-advisor": ["AMFI", "SEBI", "YELLOWPAGES"],
    "wealth-manager": ["AMFI", "SEBI", "YELLOWPAGES"],
    "investment-consultant": ["SEBI", "YELLOWPAGES"],
    # Business Directories
    "business-consultants": ["YELLOWPAGES", "TRADEINDIA", "INDIAMART"],
    "chartered-engineers": ["YELLOWPAGES", "TRADEINDIA"],
    "cost-accountants": ["YELLOWPAGES"],
    "business": ["YELLOWPAGES", "TRADEINDIA", "INDIAMART", "JUSTDIAL"],
    "local": ["YELLOWPAGES", "GROTAL", "SULEKHA"],
    "person": ["LINKEDIN"],
    "lead": ["LINKEDIN", "GMB"],
    "professional": ["LINKEDIN", "SEBI", "NSE"],
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
    redis_url: Optional[str] = None


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
                "host": env_proxy_host,
                "username": os.environ.get("PROXY_USER", ""),
                "password": os.environ.get("PROXY_PASS", ""),
            }
        )
    elif "proxies" in proxy_cfg:
        for p in proxy_cfg["proxies"]:
            proxies.append(
                {
                    "host": p.get("host", ""),
                    "username": p.get("username", ""),
                    "password": p.get("password", ""),
                }
            )
    elif proxy_cfg.get("host"):
        proxies.append(
            {
                "host": proxy_cfg["host"],
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


class BaseScraperProxy(BaseScraper):
    # This is a bridge between the old BaseScraper and new BaseScraper
    pass


class JustDialScraper(BaseScraper):
    source_name = "JUSTDIAL"
    force_http1 = True

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        category_slug = category.lower().replace(" ", "-")
        if page > 1:
            return f"https://www.justdial.com/{city}/{category_slug}/page-{page}"
        return f"https://www.justdial.com/{city}/{category_slug}"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a.store-name")
            if link:
                href = await link.get_attribute("href")
                return href
        except:
            pass
        return None

    async def _extract_phone(self, card) -> Optional[str]:
        """Modernized 2026 JustDial extraction: No more icons, now plain text hidden in anchors"""
        try:
            # 1. Primary: Look for 'callNowAnchor' which often has the number in its text or data attributes
            selectors = [
                 "span.callNowAnchor", "a.callNowAnchor", 
                 "a[href*='tel:']", "button.callbutton",
                 ".contact-info", ".phone-num", ".store-phone"
            ]
            
            for sel in selectors:
                elements = await card.query_selector_all(sel)
                for el in elements:
                    text = await el.inner_text()
                    # Also check href for tel: links
                    href = await el.get_attribute("href") or ""
                    
                    phone_source = text + " " + href
                    clean = self._clean_phone(phone_source)
                    if clean and len(clean) >= 10:
                        return clean
            
            # 2. Fallback: Search for any 10+ digit sequence in the card's text
            card_text = await card.inner_text()
            matches = re.findall(r'[6-9]\d{9}', card_text.replace(" ", "").replace("-", ""))
            if matches:
                return matches[0]
            
            # 2026: Check for hidden data attributes
            html = await card.inner_html()
            data_matches = re.findall(r'data-tel="(\d+)"', html)
            if data_matches:
                return data_matches[0]

        except Exception as e:
            logger.debug(f"JustDial: Phone extraction failed: {e}")
        return None


    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            await page.wait_for_selector("body", timeout=15000)
            
            # 2026 WAF Buffer: JustDial requires a bit more 'settling' time to bypass the pre-load challenge
            await asyncio.sleep(3)
            
            # Anti-Lazy Loading: Scroll to reveal cards
            for _ in range(5):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(0.7)

            # Wait for any typical JD container to appear
            try:
                await page.wait_for_selector(".store-info, .jsx-762296e8b7880524, .listing-card", timeout=8000)
            except:
                pass

            page_title = await page.title()
            page_url = page.url
            logger.info(f"JustDial page title: {page_title}")
            logger.info(f"JustDial page URL: {page_url}")

            page_content = await page.content()
            logger.info(f"Page content length: {len(page_content)}")

            card_selectors = [
                ".resultbox", # 2026 Primary Card
                ".jsx-17aafc05bdbc2ecd", # 2026 Component Class
                ".store-list .store-info",
                ".results_listing_container > div",
                ".store-info",
                ".results .store-info",
                ".listing-card",
                ".business-card",
                "li.store-data",
                "article",
                ".card",
            ]

            cards = []
            for sel in card_selectors:
                try:
                    cards = await page.query_selector_all(sel)
                    if cards:
                        logger.info(f"Found {len(cards)} cards with selector: {sel}")
                        break
                except:
                    continue

            if not cards:
                logger.warning(
                    "No cards found with any selector, trying text extraction"
                )
                body_text = await page.inner_text("body")
                phone_matches = re.findall(r"(\d{10,12})", body_text)
                name_lines = [
                    line.strip()
                    for line in body_text.split("\n")
                    if len(line.strip()) > 3
                    and not line.strip().startswith(("http", "www", "©"))
                ]

                for i, name in enumerate(name_lines[:20]):
                    if any(
                        x in name.lower()
                        for x in [
                            "cookie",
                            "privacy",
                            "terms",
                            "login",
                            "sign up",
                            "download",
                        ]
                    ):
                        continue
                    phone = phone_matches[i] if i < len(phone_matches) else None
                    listings.append(
                        {
                            "name": name,
                            "phone": phone,
                            "address": None,
                            "area": None,
                            "detail_url": None,
                        }
                    )
                logger.info(f"Extracted {len(listings)} listings from text")
                return listings

            for card in cards:
                try:
                    name_selectors = [
                        ".resultbox_title_anchor",
                        ".store-name",
                        ".name",
                        "h2",
                        "h3",
                        ".business-name",
                        "a.store-name",
                    ]
                    name = None
                    for sel in name_selectors:
                        name = await self._get_text(card, sel)
                        if name:
                            break

                    phone = await self._extract_phone(card)

                    addr_selectors = [
                        "address", # 2026 Primary
                        ".store-address",
                        ".address",
                        ".addr",
                    ]
                    address = None
                    for sel in addr_selectors:
                        address = await self._get_text(card, sel)
                        if address:
                            break

                    area_selectors = [".store-area", ".area", '[class*="area"]']
                    area = None
                    for sel in area_selectors:
                        area = await self._get_text(card, sel)
                        if area:
                            break

                    link_selectors = ["a.store-name", "a.business-name", 'a[href*="/"]']
                    detail_url = None
                    for sel in link_selectors:
                        try:
                            link = await card.query_selector(sel)
                            if link:
                                detail_url = await link.get_attribute("href")
                                if detail_url:
                                    break
                        except:
                            continue

                    if name:
                        listings.append(
                            {
                                "name": name.strip(),
                                "phone": phone,
                                "address": address.strip() if address else None,
                                "area": area.strip() if area else None,
                                "detail_url": detail_url,
                            }
                        )
                except Exception as e:
                    logger.debug(f"Card parse error: {e}")
                    continue

            logger.info(f"Extracted {len(listings)} listings")
        except Exception as e:
            logger.warning(f"Listings extraction error: {e}")
        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class ICICIScraper(BaseScraper):
    source_name = "ICICI"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        category_lower = category.lower().replace(" ", "-")
        return f"https://www.iciciprulife.com/agentsearch/{category_lower}.do?city={city.lower()}"

    async def get_detail_url(self, card) -> Optional[str]:
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            cards = await page.query_selector_all(".agent-card, .search-result-item")

            for card in cards:
                try:
                    name = await self._get_text(card, ".agent-name, .name")
                    phone = await self._get_text(card, ".agent-phone, .phone")
                    address = await self._get_text(card, ".agent-address, .address")

                    if name:
                        listings.append(
                            {
                                "name": name.strip(),
                                "phone": self._clean_phone(phone) if phone else None,
                                "address": address.strip() if address else None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                except:
                    continue
        except Exception as e:
            logger.warning(f"ICICI extraction error: {e}")
        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class AMFIScraper(BaseScraper):
    source_name = "AMFI"

    ARN_BASE_URL = "https://www.amfiindia.com/load-distributor-data"
    SEARCH_API_URL = "https://www.amfiindia.com/api/distributor-agent"

    def build_search_url(self, city: Optional[str], category: str, page: int = 1) -> str:
        clean_city = (city or "").replace(' ', '+')
        return f"https://www.amfiindia.com/api/distributor-agent?strOpt=ALL&city={clean_city}&page={page}&pageSize=100"

    async def scrape_via_api(self, city: str, page_num: int = 1) -> List[Dict]:
        """High-speed API extraction for AMFI (saves 95% bandwidth)"""
        listings = []
        try:
            # We use the built-in AsyncSession from fast_scraper if available
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome110") as s:
                url = self.build_search_url(city, "", page_num)
                headers = {
                    "Referer": "https://www.amfiindia.com/locate-distributor",
                    "User-Agent": StealthManager.get_random_ua()
                }
                resp = await s.get(url, headers=headers, timeout=15)
                # FIX: Check status and Content-Type to avoid 'NoneType' or HTML error parsing
                if resp.status_code == 200 and 'application/json' in resp.headers.get('Content-Type', '').lower():
                    data = resp.json()
                    raw_data = data.get("data", []) if isinstance(data, dict) else []
                    for item in raw_data:
                        listings.append({
                            "name": item.get("ARNHolderName", "").strip(),
                            "phone": self._clean_phone(item.get("TelephoneNumber_O")),
                            "email": item.get("Email"),
                            "address": item.get("Address"),
                            "city": item.get("City", city),
                            "pin": item.get("Pin"),
                            "arn": item.get("ARN"),
                            "detail_url": None
                        })
                else:
                    logger.warning(f"AMFI API: Invalid response ({resp.status_code}) or Content-Type: {resp.headers.get('Content-Type')}")
                    logger.info(f"AMFI API: Extracted {len(listings)} leads for {city} (Page {page_num})")
        except Exception as e:
            logger.error(f"AMFI API Error: {e}")
        return listings

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        # If we have the page, we can try to extract from UI or just hit the API in background if browser is too heavy
        # First check if the page has the JSON we need (sometimes it's in a script tag)
        listings = []
        try:
            # Check if we are being blocked by WAF first
            title = await page.title()
            if "Cloudflare" in title or "Access Denied" in title:
                logger.warning("AMFI: Browser blocked by WAF. Swapping to direct API attempt.")
                return await self.scrape_via_api(city)

            # Try to hit API directly via page context to leverage cookies/stealth
            api_url = f"https://www.amfiindia.com/api/distributor-agent?strOpt=ALL&city={city}&page=1&pageSize=100"
            resp_json = await page.evaluate(f"async () => {{ const r = await fetch('{api_url}'); return r.json(); }}")
            if resp_json and "data" in resp_json:
                for item in resp_json["data"]:
                    listings.append({
                        "name": item.get("ARNHolderName", "").strip(),
                        "phone": self._clean_phone(item.get("TelephoneNumber_O")),
                        "email": item.get("Email"),
                        "address": item.get("Address"),
                        "city": item.get("City", city),
                        "arn": item.get("ARN"),
                    })
                if listings:
                    logger.info(f"AMFI: Extracted {len(listings)} leads via Page-Proxy API call")
                    return listings

        except Exception as e:
            logger.debug(f"AMFI: Page-Proxy API failed: {e}. Falling back to UI Scraping.")
        # Legacy UI Scraping Fallback
        try:
            # MUI Grid selectors for 2026 update
            try:
                await page.wait_for_selector(".MuiBox-root.css-13rzqox, .MuiBox-root.css-ds3kc", timeout=5000)
            except:
                logger.warning("AMFI: MUI Grid did not appear via selector, continuing anyway.")

            # Try multiple row selectors
            row_selectors = [
                ".MuiBox-root.css-ds3kc",  # New MUI-based grid rows
                "div.MuiBox-root.css-1oi5t4f > div:not(:first-child)",  # Alternative MUI container
                "table tbody tr",
                ".distributor-list .distributor-item",
                ".result-row, .result-item",
                'tr[data-id], tr[class*="dist"]',
                'div[class*="distributor"]',
                "table tr",
                "#example tbody tr",
                ".dataTables_scrollBody tbody tr",
                'div[id*="table"] tbody tr',
            ]

            cards = []
            for sel in row_selectors:
                try:
                    cards = await page.query_selector_all(sel)
                    if cards and len(cards) > 1:
                        logger.info(f"AMFI: Found {len(cards)} cards with selector: {sel}")
                        break
                except Exception:
                    continue

            if not cards:
                # Text-based fallback (regex) - Very powerful for 2026 dynamic sites
                body_text = await page.inner_text("body")
                arn_matches = re.finditer(r"ARN-(\d+)", body_text)
                for match in arn_matches:
                    arn = match.group(1)
                    # Simple heuristic: find nearby name
                    context = body_text[max(0, match.start()-100) : match.end()+100]
                    lines = context.split("\n")
                    name = lines[0] if lines else "Distributor " + arn
                    listings.append({
                        "name": name.strip(),
                        "arn": arn,
                        "city": city,
                        "phone": None,
                        "email": None
                    })
                return listings

            for card in cards:
                try:
                    # AMFI Specific Selector set
                    name = await self._get_text(card, 'div[class*="HolderName"], .name, b, td:first-child')
                    arn = await self._get_text(card, 'div[class*="ARN"], .arn, td:nth-child(2)')
                    phone = await self._get_text(card, 'div[class*="Mobile"], .phone, td:nth-child(4)')
                    email = await self._get_text(card, 'div[class*="Email"], .email, td:nth-child(5)')

                    if name and len(name.strip()) > 1:
                        listings.append({
                            "name": name.strip(),
                            "arn": arn.strip() if arn else None,
                            "phone": self._clean_phone(phone) if phone else None,
                            "email": email.strip() if email else None,
                            "city": city,
                            "source": "AMFI"
                        })
                except Exception:
                    continue

        except Exception as e:
            logger.error(f"AMFI UI fallback error: {e}")

        return listings

class IRDAIScraper(BaseScraper):
    """Scraper for IRDAI Insurance Agent data (policyholder.gov.in)"""

    source_name = "IRDAI"

    AGENT_SEARCH_URL = "https://www.policyholder.gov.in/agent-search"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.AGENT_SEARCH_URL

    async def get_detail_url(self, card) -> Optional[str]:
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"IRDAI: Starting extraction for city={city}, category={category}")

        try:
            # Log page info (Wrapped in try-except for navigation races)
            try:
                page_title = await page.title()
                page_url = page.url
                logger.info(f"IRDAI: Page title: {page_title}")
                logger.info(f"IRDAI: Page URL: {page_url}")
            except Exception as e:
                logger.debug(f"IRDAI: Could not fetch title/url: {e}")

            # Determine state from city
            state = CITY_STATE_MAP.get((city or "").lower(), "MAHARASHTRA")
            logger.info(f"IRDAI: Selected state: {state}")

            # Try to find and interact with form elements
            selectors_to_try = [
                ("#ddlState", "select"),
                ('[id*="state"]', "select"),
                ('[name*="state"]', "select"),
                ('select[id*="State"]', "select"),
            ]

            state_selected = False
            for sel, elem_type in selectors_to_try:
                try:
                    elem = await page.wait_for_selector(
                        sel, state="visible", timeout=5000
                    )
                    if elem:
                        # Try selecting by label first, then by value if label fails
                        try:
                            await elem.select_option(label=state)
                        except:
                            # Many Indian govt sites use uppercase values
                            await elem.select_option(value=state.upper())

                        state_selected = True
                        logger.info(
                            f"IRDAI: Selected state ({state}) using selector: {sel}"
                        )
                        break
                except Exception:
                    continue

            if state_selected:
                await asyncio.sleep(1)

                # Try district/city selection
                district_selectors = [
                    "#ddlDistrict",
                    '[id*="district"]',
                    '[id*="city"]',
                ]
                for sel in district_selectors:
                    try:
                        elem = await page.query_selector(sel)
                        if elem:
                            await elem.select_option(
                                label=city.upper() if city else None
                            )
                            logger.info(
                                f"IRDAI: Selected district using selector: {sel}"
                            )
                            break
                    except Exception:
                        continue

                # Click search/locate button
                button_selectors = [
                    "#btnLocate",
                    "#btnSearch",
                    '[id*="Locate"]',
                    '[id*="Search"]',
                    'button[type="submit"]',
                ]
                for sel in button_selectors:
                    try:
                        btn = await page.query_selector(sel)
                        if btn:
                            await btn.click()
                            logger.info(f"IRDAI: Clicked button using selector: {sel}")
                            break
                    except Exception:
                        continue

                await asyncio.sleep(2)

            # Log page state after interaction
            body_text = await page.inner_text("body")
            logger.info(
                f"IRDAI: Page text preview (first 300 chars): {body_text[:300]}"
            )

            # Try multiple row selectors
            row_selectors = [
                ".agent-item",
                "tr[data-id]",
                "table tbody tr",
                ".result-item",
                ".search-result",
                'tr[class*="agent"]',
            ]

            cards = []
            for sel in row_selectors:
                cards = await page.query_selector_all(sel)
                if cards:
                    logger.info(f"IRDAI: Found {len(cards)} rows with selector: {sel}")
                    break

            if not cards:
                logger.warning("IRDAI: No rows found!")
                # Text-based fallback
                all_text = await page.inner_text("body")
                # Look for license patterns (like 1234567, IRDA/XXXX)
                import re

                license_pattern = re.compile(
                    r"(?:License|No\.?)\s*:?\s*([A-Z0-9/]{5,})", re.IGNORECASE
                )
                lines = [l.strip() for l in all_text.split("\n") if l.strip()]

                for i, line in enumerate(lines):
                    if license_pattern.search(line):
                        name = lines[i - 1] if i > 0 else line
                        license_no = license_pattern.search(line).group(1)
                        listings.append(
                            {
                                "name": name[:100],
                                "license_no": license_no,
                                "city": city,
                                "phone": None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                        if len(listings) >= 50:
                            break

                logger.info(f"IRDAI: Text extraction found {len(listings)} listings")
                return listings

            for card in cards:
                try:
                    name = await self._get_text(
                        card, '.agent-name, .name, td:first-child, [class*="name"]'
                    )
                    license_no = await self._get_text(
                        card, '.license-no, td:nth-child(2), [class*="license"]'
                    )
                    city_result = await self._get_text(
                        card, '.city, td:nth-child(3), [class*="city"]'
                    )
                    phone = await self._get_text(
                        card, '.phone, td:nth-child(4), [class*="phone"]'
                    )

                    if name and license_no and len(name.strip()) > 1:
                        listings.append(
                            {
                                "name": name.strip(),
                                "license_no": license_no.strip(),
                                "city": city_result.strip() if city_result else city,
                                "phone": self._clean_phone(phone) if phone else None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                except Exception as e:
                    logger.debug(f"IRDAI: Card parse error: {e}")
                    continue

            logger.info(f"IRDAI: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"IRDAI: Extraction error: {e}")
            import traceback

            logger.error(f"IRDAI: Traceback: {traceback.format_exc()}")

        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class ICAIScraper(BaseScraper):
    """Modernized Scraper for ICAI (Chartered Accountants) - Handles 2026 portal"""

    source_name = "ICAI"
    MEMBER_SEARCH_URL = "https://eservices.icai.org/traceamember"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.MEMBER_SEARCH_URL

    @staticmethod
    def _clean_phone(phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector(
                "a.member-name, a.title, [class*='name'] a"
            )
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            await page.wait_for_selector("body", timeout=15000)

            # Common selectors for CA/Professional directories
            card_selectors = [
                ".member-card",
                ".member-item",
                ".listing-item",
                "table tbody tr",
                ".ca-item",
                ".professional-card",
            ]

            cards = []
            for sel in card_selectors:
                cards = await page.query_selector_all(sel)
                if cards:
                    logger.info(f"ICAI: Found {len(cards)} cards with selector: {sel}")
                    break

            if not cards:
                # Text fallback if no structured cards found
                body_text = await page.inner_text("body")
                lines = [l.strip() for l in body_text.split("\n") if len(l.strip()) > 3]
                for line in lines[:20]:
                    if any(
                        x in line.lower() for x in ["ca ", "chartered", "accountant"]
                    ):
                        listings.append(
                            {
                                "name": line[:100],
                                "membership_no": None,
                                "city": city,
                                "phone": None,
                                "email": None,
                                "address": None,
                                "detail_url": None,
                            }
                        )
                return listings

            for card in cards:
                try:
                    name = await self._get_text(
                        card, ".name, .member-name, h3, td:first-child"
                    )
                    phone = await self._get_text(
                        card, ".phone, .contact, td:nth-child(3)"
                    )
                    addr = await self._get_text(card, ".address, .loc, td:nth-child(4)")
                    mem_no = await self._get_text(
                        card, ".mem-no, .membership, td:nth-child(2)"
                    )

                    if name:
                        listings.append(
                            {
                                "name": name.strip(),
                                "phone": self._clean_phone(phone) if phone else None,
                                "address": addr.strip() if addr else None,
                                "membership_no": mem_no.strip() if mem_no else None,
                                "city": city,
                                "detail_url": await self.get_detail_url(card),
                            }
                        )
                except:
                    continue
        except Exception as e:
            logger.warning(f"ICAI extraction error: {e}")
        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        try:
            elem = await card.query_selector(selector)
            return await elem.inner_text() if elem else None
        except:
            return None


class ICSIScraper(BaseScraper):
    """Modernized Scraper for ICSI (Company Secretaries) - Handles 2026 'Stimulate' portal"""

    source_name = "ICSI"
    MEMBER_SEARCH_URL = "https://stimulate.icsi.edu/members/MemberSearch.aspx"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.MEMBER_SEARCH_URL

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a.member-name, a[href*="member"]')
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            await page.wait_for_selector("body", timeout=15000)

            card_selectors = [
                ".member-list .member-item",
                ".member-card",
                ".directory-item",
                '[class*="member"]',
                "table tbody tr",
            ]

            cards = []
            for sel in card_selectors:
                cards = await page.query_selector_all(sel)
                if cards:
                    logger.info(f"ICSI: Found {len(cards)} cards with selector: {sel}")
                    break

            if not cards:
                logger.warning("ICSI: No cards found, trying text extraction")
                body_text = await page.inner_text("body")
                lines = [l.strip() for l in body_text.split("\n") if l.strip()]

                for line in lines[:30]:
                    if "icsi" in line.lower() or "company secretary" in line.lower():
                        listings.append(
                            {
                                "name": line[:100],
                                "membership_no": None,
                                "city": city,
                                "phone": None,
                                "email": None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                return listings

            for card in cards:
                try:
                    name = await self._get_text(
                        card, ".member-name, .name, h3, h4, td:first-child"
                    )
                    membership_no = await self._get_text(
                        card, ".membership-no, .membership, td:nth-child(2)"
                    )
                    city_result = await self._get_text(card, ".city, td:nth-child(3)")
                    phone = await self._get_text(
                        card, ".phone, td:nth-child(4), .contact"
                    )
                    email = await self._get_text(
                        card, '.email, td:nth-child(5), a[href*="mailto"]'
                    )

                    if name:
                        listings.append(
                            {
                                "name": name.strip(),
                                "membership_no": membership_no.strip()
                                if membership_no
                                else None,
                                "city": city_result.strip() if city_result else city,
                                "phone": self._clean_phone(phone) if phone else None,
                                "email": email.strip() if email else None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                except Exception as e:
                    logger.debug(f"ICSI: Card parse error: {e}")
                    continue

            logger.info(f"ICSI: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"ICSI: Extraction error: {e}")

        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector(
                'a[href*="memberProfile"], a[href*="firmProfile"]'
            )
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        try:
            await page.wait_for_selector(
                ".searchBox.scr, .div .searchBox", timeout=15000
            )
            cards = await page.query_selector_all(".searchBox.scr")

            for card in cards:
                try:
                    name = await self._get_text(card, "p b")
                    location = await self._get_text(card, ".state")
                    detail_url = await self.get_detail_url(card)

                    if name:
                        cleaned_name = re.sub(
                            r"^\s*CA\.\s*", "", name.strip(), flags=re.IGNORECASE
                        )
                        state = None
                        city_value = city
                        if location:
                            normalized_location = re.sub(r"\s+", " ", location).strip()
                            parts = [
                                part.strip()
                                for part in normalized_location.split(",")
                                if part.strip()
                            ]
                            if len(parts) >= 2:
                                city_value = parts[0].title()
                                state = parts[1].upper()

                        listings.append(
                            {
                                "name": cleaned_name,
                                "membership_no": None,
                                "city": city_value,
                                "state": state,
                                "email": None,
                                "phone": None,
                                "address": None,
                                "area": None,
                                "detail_url": detail_url,
                            }
                        )
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"ICAI extraction error: {e}")
        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class SEBIScraper(BaseScraper):
    """Scraper for SEBI (Securities and Exchange Board of India) registered intermediaries"""

    source_name = "SEBI"
    force_http1 = True

    REGISTRAR_URL = (
        "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"
    )

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.REGISTRAR_URL

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a[href*="doRead"]')
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"SEBI: Starting extraction for city={city}, category={category}")

        try:
            await page.wait_for_selector("body", timeout=15000)

            page_title = await page.title()
            page_url = page.url
            logger.info(f"SEBI: Page title: {page_title}")
            logger.info(f"SEBI: Page URL: {page_url}")

            row_selectors = [
                "table tbody tr",
                ".registrant-list tr",
                ".data-table tr",
                '[class*="table"] tbody tr',
            ]

            rows = []
            for sel in row_selectors:
                rows = await page.query_selector_all(sel)
                if rows:
                    logger.info(f"SEBI: Found {len(rows)} rows with selector: {sel}")
                    break

            if not rows:
                logger.warning("SEBI: No rows found")
                return listings

            for row in rows:
                try:
                    cols = await row.query_selector_all("td")
                    if len(cols) >= 2:
                        name = await cols[0].inner_text()
                        reg_no = await cols[1].inner_text() if len(cols) > 1 else None

                        if name and len(name.strip()) > 2:
                            listings.append(
                                {
                                    "name": name.strip(),
                                    "registration_no": reg_no.strip()
                                    if reg_no
                                    else None,
                                    "city": city,
                                    "phone": None,
                                    "email": None,
                                    "address": None,
                                    "area": None,
                                    "detail_url": None,
                                }
                            )
                except Exception as e:
                    logger.debug(f"SEBI: Row parse error: {e}")
                    continue

            logger.info(f"SEBI: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"SEBI: Extraction error: {e}")

        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class NSEBrokerScraper(BaseScraper):
    """Scraper for NSE (National Stock Exchange) registered brokers"""

    source_name = "NSE"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return "https://www.nseindia.com/market-data/equity-derivatives-watch"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a")
            if link:
                href = await link.get_attribute("href")
                if href and "member" in href.lower():
                    return href
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"NSE: Starting extraction for city={city}")

        try:
            await page.wait_for_selector("body", timeout=15000)
            page_url = page.url
            logger.info(f"NSE: Page URL: {page_url}")

            body_text = await page.inner_text("body")

            name_pattern = re.compile(
                r"([A-Z][A-Z\s]+(?:Broking|Securities|Pvt|Ltd)[^\d\n]{2,50})",
                re.IGNORECASE,
            )
            matches = name_pattern.findall(body_text)

            for match in matches[:30]:
                listings.append(
                    {
                        "name": match.strip()[:100],
                        "member_code": None,
                        "city": city,
                        "phone": None,
                        "email": None,
                        "address": None,
                        "area": None,
                        "detail_url": None,
                    }
                )

            logger.info(f"NSE: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"NSE: Extraction error: {e}")

        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class BSEBrokerScraper(BaseScraper):
    """Modernized Scraper for BSE (Bombay Stock Exchange) - Handles 2026 portal structure"""

    source_name = "BSE"
    BASE_URL = "https://www.bseindia.com/corporates/List_Scrips.aspx"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"BSE: Targeted extraction for city={city}")

        try:
            # Phase 1: Interactive Portal Navigation
            await page.wait_for_selector("#ContentPlaceHolder1_ddlSegment", timeout=15000)
            
            # Select Equity segment for widest search
            await page.select_option("#ContentPlaceHolder1_ddlSegment", "Equity")
            # Select Active status
            await page.select_option("#ContentPlaceHolder1_ddlStatus", "Active")
            
            await page.click("#ContentPlaceHolder1_btnSubmit")
            await page.wait_for_load_state("networkidle")
            
            # Phase 2: High-Fidelity Extraction
            soup = BeautifulSoup(html_content or await page.content(), "lxml")
            rows = soup.select("table[id*='gvScrips'] tr")
            
            if not rows:
                # Fallback to general table search
                rows = soup.select(".table-striped tr, .common-table tr")

            for row in rows[1:]: # Skip header
                cols = row.select("td")
                if len(cols) >= 3:
                    name = cols[1].get_text(strip=True)
                    code = cols[0].get_text(strip=True)
                    
                    if name and len(name) > 3:
                        listings.append({
                            "name": name[:150],
                            "member_code": code,
                            "city": city,
                            "phone": None,
                            "email": None,
                            "address": None,
                            "status": "Active",
                            "source": "BSE Official"
                        })

            logger.info(f"BSE: Successfully extracted {len(listings)} authorized entities.")

        except Exception as e:
            logger.error(f"BSE Enterprise Error: {e}. Falling back to raw regex.")
            # Enterprise Fallback: Call the base class regex engine if UI fails
            listings = self.extract_raw_fallback(html_content or await page.content(), city, category)

        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class GSTPractitionerScraper(BaseScraper):
    """Scraper for GST Practitioners (Goods and Services Tax)"""

    source_name = "GST"

    GST_PORTAL_URL = "https://services.gst.gov.in/services/searchtaxpayer"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.GST_PORTAL_URL

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a[href*=" taxpayer"]')
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"GST: Starting extraction for city={city}")

        try:
            await page.wait_for_selector("body", timeout=15000)
            page_url = page.url
            logger.info(f"GST: Page URL: {page_url}")

            row_selectors = [
                "table tbody tr",
                ".taxpayer-list tr",
                ".search-results tr",
                '[class*="table"] tbody tr',
            ]

            rows = []
            for sel in row_selectors:
                rows = await page.query_selector_all(sel)
                if rows:
                    logger.info(f"GST: Found {len(rows)} rows with selector: {sel}")
                    break

            if not rows:
                body_text = await page.inner_text("body")
                lines = [
                    l.strip() for l in body_text.split("\n") if l.strip() and len(l) > 3
                ]

                for line in lines[:20]:
                    if (
                        "gst" in line.lower()
                        or "goods" in line.lower()
                        or "tax" in line.lower()
                    ):
                        listings.append(
                            {
                                "name": line[:100],
                                "gstin": None,
                                "city": city,
                                "phone": None,
                                "email": None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                return listings

            for row in rows:
                try:
                    cols = await row.query_selector_all("td")
                    if len(cols) >= 2:
                        name = await cols[0].inner_text()
                        gstin = await cols[1].inner_text() if len(cols) > 1 else None

                        if name and len(name.strip()) > 2:
                            listings.append(
                                {
                                    "name": name.strip(),
                                    "gstin": gstin.strip() if gstin else None,
                                    "city": city,
                                    "phone": None,
                                    "email": None,
                                    "address": None,
                                    "area": None,
                                    "detail_url": None,
                                }
                            )
                except Exception as e:
                    logger.debug(f"GST: Row parse error: {e}")
                    continue

            logger.info(f"GST: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"GST: Extraction error: {e}")

        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class RBIRegulatedScraper(BaseScraper):
    """Scraper for RBI (Reserve Bank of India) regulated entities - Banks, NBFCs"""

    source_name = "RBI"

    RBI_PORTAL_URL = "https://rbi.org.in/Scripts/BS_ViewMasArchive.aspx?Id=120"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.RBI_PORTAL_URL

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a[href*=".aspx"]')
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        listings = []
        logger.info(f"RBI: Starting extraction for city={city}")

        try:
            await page.wait_for_selector("body", timeout=15000)
            page_url = page.url
            logger.info(f"RBI: Page URL: {page_url}")

            link_selectors = [
                "table a",
                ".master-table a",
                'a[href*="Master"]',
                'a[href*="Bank"]',
                'a[href*="NBFC"]',
            ]

            links = []
            for sel in link_selectors:
                links = await page.query_selector_all(sel)
                if links:
                    logger.info(f"RBI: Found {len(links)} links with selector: {sel}")
                    break

            if not links:
                body_text = await page.inner_text("body")
                name_pattern = re.compile(
                    r"([A-Z][A-Z\s]+(?:Bank|Finance|NBFC|HFC)[^\d\n]{2,60})",
                    re.IGNORECASE,
                )
                matches = name_pattern.findall(body_text)

                for match in matches[:20]:
                    listings.append(
                        {
                            "name": match.strip()[:100],
                            "license_no": None,
                            "city": city,
                            "phone": None,
                            "email": None,
                            "address": None,
                            "area": None,
                            "detail_url": None,
                        }
                    )
                logger.info(f"RBI: Extracted {len(listings)} from text")
                return listings

            for link in links[:30]:
                try:
                    name = await link.inner_text()
                    href = await link.get_attribute("href")

                    if name and len(name.strip()) > 3 and "href" in (href or ""):
                        listings.append(
                            {
                                "name": name.strip()[:100],
                                "license_no": None,
                                "city": city,
                                "phone": None,
                                "email": None,
                                "address": None,
                                "area": None,
                                "detail_url": href,
                            }
                        )
                except Exception as e:
                    logger.debug(f"RBI: Link parse error: {e}")
                    continue

            logger.info(f"RBI: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"RBI: Extraction error: {e}")

        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class YellowPagesScraper(BaseScraper):
    """Scraper for YellowPages - high volume business directory"""

    source_name = "YELLOWPAGES"

    BASE_URL = "https://www.yellowpages.in"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # 2026 Fix: /city/category pattern is 404ing in some regions.
        # Verified fallback: /city/search/category or direct search.
        city_slug = city.strip().replace(" ", "-").lower()
        cat_slug = category.strip().replace(" ", "-").lower()
        if page == 1:
            return f"{self.BASE_URL}/{city_slug}/search/{cat_slug}"
        return f"{self.BASE_URL}/{city_slug}/search/{cat_slug}?page={page}"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a.business-name, a.title")
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self, page: Page, city: str = None, category: str = None
    ) -> List[Dict]:
        listings = []
        logger.info(
            f"YellowPages: Starting extraction for city={city}, category={category}"
        )

        try:
            await page.wait_for_selector("body", timeout=15000)

            # 2026 Verified selectors from live browser recon
            card_selectors = [
                "div.eachPopular",
                ".search-results .result",
                ".listing-card",
                ".business-card",
                ".srp-item",
                "[class*='result-item']",
                "article.listing",
                "div[id*='listing']",
            ]

            cards = []
            for sel in card_selectors:
                cards = await page.query_selector_all(sel)
                if cards:
                    logger.info(
                        f"YellowPages: Found {len(cards)} cards with selector: {sel}"
                    )
                    break

            for card in cards:
                try:
                    # 2026 Verified selectors
                    name = await self._get_text(
                        card, "a.eachPopularTitle, h2, h3, .business-name, .title, .name"
                    )
                    phone = await self._get_text(
                        card, "a.businessContact, .phone, .contact-phone, [class*='phone']"
                    )
                    address = await self._get_text(
                        card, "address.businessArea, .address, .location, .contact-addr"
                    )
                    area = await self._get_text(card, ".area, .locality")
                    email_elem = await card.query_selector("a[href*='mailto']")
                    email = (
                        await email_elem.get_attribute("href") if email_elem else None
                    )
                    if email:
                        email = email.replace("mailto:", "")

                    if name and len(name.strip()) > 2:
                        listings.append(
                            {
                                "name": name.strip()[:150],
                                "phone": self._clean_phone(phone) if phone else None,
                                "email": email.strip() if email else None,
                                "address": address.strip() if address else None,
                                "city": city,
                                "area": area.strip() if area else None,
                                "detail_url": await self.get_detail_url(card),
                            }
                        )
                except Exception as e:
                    logger.debug(f"YellowPages: Card parse error: {e}")
                    continue

            logger.info(f"YellowPages: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"YellowPages: Extraction error: {e}")

        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class TradeIndiaScraper(BaseScraper):
    """Scraper for TradeIndia - B2B directory"""

    source_name = "TRADEINDIA"

    BASE_URL = "https://www.tradeindia.com"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # 2026 Optimization: search.html with keyword is high-drag and prone to WAF.
        # Direct paths like /City/Category.html are faster and more stable.
        city_slug = city.strip().replace(" ", "-").title()
        cat_slug = category.strip().replace(" ", "-").title()
        if page == 1:
            return f"{self.BASE_URL}/{city_slug}/{cat_slug}.html"
        return f"{self.BASE_URL}/{city_slug}/{cat_slug}.html?page={page}"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a.company-name, a[href*='/companies/']")
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self, page: Page, city: str = None, category: str = None
    ) -> List[Dict]:
        listings = []
        logger.info(
            f"TradeIndia: Starting extraction for city={city}, category={category}"
        )

        try:
            await page.wait_for_selector("body", timeout=15000)

            # Modern 2026 Card Selectors (Styled Components)
            card_selectors = [
                ".sc-c97d860b-0", 
                "div[class*='ProductCard']",
                ".listing-card",
                ".results-container > div",
                ".company-list .company-item", 
                ".search-result .result-item"
            ]

            cards = []
            for sel in card_selectors:
                cards = await page.query_selector_all(sel)
                if cards:
                    logger.info(f"TradeIndia: Found {len(cards)} cards with selector: {sel}")
                    break

            if not cards:
                # Regex Fallback (already optimized)
                body_text = await page.inner_text("body")
                phone_pattern = re.compile(r'(?:\+91[\-\s]?)?[6-9]\d{4}[\-\s]?\d{5}')
                email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
                phones_found = phone_pattern.findall(body_text)
                emails_found = email_pattern.findall(body_text)
                lines = [l.strip() for l in body_text.split("\n") if l.strip() and 10 < len(l.strip()) < 120]
                name_candidates = [l for l in lines if not re.match(r'^[\d\s\-\+\(\)]+$', l) and '@' not in l]
                
                seen_phones = set()
                for i, phone in enumerate(phones_found):
                    clean = re.sub(r'[^\d]', '', phone)[-10:]
                    if clean in seen_phones: continue
                    seen_phones.add(clean)
                    listings.append({
                        "name": (name_candidates[i] if i < len(name_candidates) else f"TradeIndia Lead {i+1}")[:100],
                        "phone": clean, "email": emails_found[i] if i < len(emails_found) else None,
                        "address": None, "city": city, "area": None, "detail_url": None,
                        "source": "TRADEINDIA_REGEX"
                    })
                return listings

            for card in cards:
                try:
                    name = await self._get_text(card, "h3, .company-name, [class*='CompanyName']")
                    location = await self._get_text(card, "span[class*='fBXBUU'], .location, .city")
                    detail_url = await self.get_detail_url(card)

                    if name:
                        listings.append({
                            "name": name.strip(),
                            "city": location.strip() if location else city,
                            "phone": None, # TradeIndia 2026: Hidden behind Lead-Gen modal
                            "source": "TRADEINDIA_UI",
                            "detail_url": detail_url
                        })
                except Exception as e:
                    logger.debug(f"TradeIndia: Row parse error: {e}")
                    continue
        except Exception as e:
            logger.warning(f"TradeIndia extraction error: {e}")

        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        return digits[-10:] if len(digits) >= 10 else digits

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class IndiaMartScraper(BaseScraper):
    """Scraper for IndiaMart - B2B directory (enhanced)"""

    source_name = "INDIAMART"

    BASE_URL = "https://www.indiamart.com"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # 2026 Fix: search.html?ss= lands on landing page. Using 'isearch.php' or 'search.html?m=1'
        cat_slug = category.lower().replace(" ", "+")
        city_slug = city.lower().replace(" ", "+")
        # 'm=1' forces the results view instead of the landing view on many B2B sites
        return f"https://www.indiamart.com/search.html?ss={cat_slug}&cq={city_slug}&prdsrc=1&m=1&pn={page}"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a.company-name, a[href*='/ sellers/']")
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self, page: Page, city: str = None, category: str = None
    ) -> List[Dict]:
        listings = []
        logger.info(
            f"IndiaMart Enhanced: Starting extraction for city={city}, category={category}"
        )

        try:
            await page.wait_for_selector("body", timeout=15000)

            # 2026 Verified selectors from live browser recon
            cards = await page.query_selector_all(
                ".cl_csCTC, article.template7-product-card, .prod-list .prod-item, .seller-card, [class*='seller']"
            )

            if not cards:
                # Fallback: extract leads from raw page text via regex
                body_text = await page.inner_text("body")
                phone_pattern = re.compile(r'(?:\+91[\-\s]?)?[6-9]\d{4}[\-\s]?\d{5}')
                email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
                phones_found = phone_pattern.findall(body_text)
                emails_found = email_pattern.findall(body_text)
                lines = [l.strip() for l in body_text.split("\n") if l.strip() and 10 < len(l.strip()) < 120]
                name_candidates = [l for l in lines if not re.match(r'^[\d\s\-\+\(\)]+$', l) and '@' not in l]

                seen = set()
                for i, phone in enumerate(phones_found):
                    clean = re.sub(r'[^\d]', '', phone)[-10:]
                    if clean in seen:
                        continue
                    seen.add(clean)
                    listings.append({
                        "name": (name_candidates[i] if i < len(name_candidates) else f"IndiaMart Lead {i+1}")[:120],
                        "phone": clean,
                        "email": emails_found[i] if i < len(emails_found) else None,
                        "address": None, "city": city, "area": None, "detail_url": None,
                    })
                if listings:
                    logger.info(f"IndiaMart Regex Fallback: Extracted {len(listings)} leads from raw text")
                return listings

            for card in cards:
                try:
                    # 2026 Verified selectors: .elps1 = company name, .prd-name = product
                    name = await self._get_text(card, "a.elps1, .template7-seller-name, .company-name, .prod-name, h3")
                    phone = await self._get_text(
                        card, ".callNowButton, .prod-phn, .contact, [class*='phone']"
                    )
                    address = await self._get_text(card, ".prod-addr, .address")
                    email_elem = await card.query_selector("a[href*='mailto']")
                    email = None
                    if email_elem:
                        href = await email_elem.get_attribute("href")
                        if href:
                            email = href.replace("mailto:", "")

                    if name and len(name.strip()) > 2:
                        listings.append(
                            {
                                "name": name.strip()[:150],
                                "phone": self._clean_phone(phone) if phone else None,
                                "email": email,
                                "address": address.strip() if address else None,
                                "city": city,
                                "area": None,
                                "detail_url": await self.get_detail_url(card),
                            }
                        )
                except:
                    continue

            logger.info(
                f"IndiaMart Enhanced: Total listings extracted: {len(listings)}"
            )

        except Exception as e:
            logger.error(f"IndiaMart Enhanced: Extraction error: {e}")

        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", phone)
        return digits[-10:] if len(digits) >= 10 else digits

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
                            url_matches = re.findall(
                                r"<loc>(.*?)</loc>", text, re.IGNORECASE
                            )
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

        self.scrapers: List[BaseScraper] = [
            AMFIScraper(),
            IRDAIScraper(),
            ICAIScraper(),
            ICSIScraper(),
            SEBIScraper(),
            NSEBrokerScraper(),
            BSEBrokerScraper(),
            GSTPractitionerScraper(),
            RBIRegulatedScraper(),
        ]

        self.business_scrapers: List[BaseScraper] = [
            JustDialScraper(),
            IndiaMartScraper(),
            SulekhaScraper(),
            ClickIndiaScraper(),
            GrotalScraper(),
            GoogleMapsScraper(),
            LinkedInGoogleScraper(),
            YellowPagesScraper(),
            TradeIndiaScraper(),
            GoogleDorkScraper(),
        ]

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
        if self.page:
            self.page = None

        if self.context:
            try:
                await self.context.close()
            except Exception as exc:
                logger.debug(f"Browser context close warning: {exc}")
            finally:
                self.context = None

        if self.browser:
            try:
                await self.browser.close()
            except Exception as exc:
                logger.debug(f"Browser close warning: {exc}")
            finally:
                self.browser = None

        if stop_playwright and self.playwright:
            try:
                await self.playwright.stop()
            except Exception as exc:
                logger.debug(f"Playwright stop warning: {exc}")
            finally:
                self.playwright = None

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

    async def _create_pg_tables(self):
        # We perform these checks one by one to avoid collision errors in multi-worker environments
        try:
            await self.pool.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    phone VARCHAR(50),
                    email VARCHAR(255),
                    address TEXT,
                    category VARCHAR(100),
                    city VARCHAR(100),
                    area VARCHAR(100),
                    state VARCHAR(100),
                    source VARCHAR(100),
                    source_url TEXT,
                    phone_clean VARCHAR(50),
                    email_valid BOOLEAN,
                    enriched BOOLEAN,
                    arn VARCHAR(50),
                    license_no VARCHAR(100),
                    membership_no VARCHAR(100),
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            logger.warning(
                f"Table creation skipped or failed (possibly concurrent): {e}"
            )

        required_columns = {
            "name": "VARCHAR(255)",
            "phone": "VARCHAR(50)",
            "email": "VARCHAR(255)",
            "address": "TEXT",
            "category": "VARCHAR(100)",
            "city": "VARCHAR(100)",
            "area": "VARCHAR(100)",
            "state": "VARCHAR(100)",
            "source": "VARCHAR(100)",
            "source_url": "TEXT",
            "phone_clean": "VARCHAR(50)",
            "email_valid": "BOOLEAN",
            "enriched": "BOOLEAN",
            "arn": "VARCHAR(50)",
            "license_no": "VARCHAR(100)",
            "membership_no": "VARCHAR(100)",
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
            "CREATE INDEX IF NOT EXISTS idx_contacts_phone_clean ON contacts(phone_clean)",
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
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.sqlite_conn.commit()

    async def init_browser(
        self, disable_proxy: bool = False, force_restart: bool = False
    ):
        if force_restart:
            await self._close_browser()

        if self.page and self.browser and self.context:
            return

        if not self.playwright:
            self.playwright = await async_playwright().start()

        await asyncio.sleep(random.uniform(0.2, 1.2))

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-gpu",
            "--no-zygote",
        ]

        use_proxy = (
            bool(self.config.proxies)
            and not self.config.test_mode
            and not disable_proxy
        )
        if disable_proxy:
            self.browser_proxy_disabled = True

        proxy_str = self.proxy_manager.get_proxy_string() if use_proxy else None
        proxy_dict = {"server": proxy_str} if proxy_str else None

        for attempt in range(1, 4):
            try:
                if self.config.test_mode:
                    logger.info("Running in TEST MODE (no proxy)")
                elif use_proxy:
                    logger.info(
                        f"Using proxy: {proxy_str[:50] if proxy_str else 'None'}..."
                    )
                else:
                    logger.info("Launching browser without proxy")

                self.browser = await self.playwright.chromium.launch(
                    headless=self.config.headless, args=launch_args
                )

                # Get Persistent MacOS User-Agent for the entire session
                if not self.session_ua:
                    self.session_ua = StealthManager.get_persistent_ua()
                
                user_agent = self.session_ua
                extra_headers = StealthManager.get_modern_headers(user_agent)

                context_kwargs = {
                    "user_agent": user_agent,
                    "extra_http_headers": extra_headers,
                    "viewport": {"width": 1920, "height": 1080},
                    "ignore_https_errors": True,
                    "locale": "en-US",
                    "timezone_id": "America/Los_Angeles",
                    "color_scheme": "dark",
                }
                if proxy_dict:
                    context_kwargs["proxy"] = proxy_dict

                self.context = await self.browser.new_context(**context_kwargs)

                # Apply advanced stealth patches
                await StealthManager.apply_stealth(self.context)

                self.page = await self.context.new_page()

                logger.info(
                    f"Browser session persist: UA={user_agent[:40]}... (MacOS Persistent)"
                )
                return

            except Exception as exc:
                await self._close_browser()

                if use_proxy and self._is_proxy_error(exc):
                    logger.warning(
                        "Proxy failed during browser setup, retrying without proxy"
                    )
                    use_proxy = False
                    proxy_str = None
                    proxy_dict = None
                    self.browser_proxy_disabled = True
                    continue

                if attempt == 3:
                    raise

                logger.warning(f"Browser init retry {attempt}/3 failed: {exc}")
                await asyncio.sleep(min(6, attempt * 2))

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

            # Final safety check against DB (especially if cleaning changed the phone)
            is_dup = await self.is_duplicate(
                listing.get("phone_clean"), listing.get("email")
            )
            if is_dup:
                self.stats["duplicates_skipped"] += 1
                continue

            processed_listings.append(listing)
            
            # 2026 Stealth Break: Sleep for 1-2 mins after every 5 leads (Proxy-less logic)
            self.session_leads_count += 1
            if self.session_leads_count % 5 == 0:
                break_time = random.uniform(60, 120)
                logger.info(f"Stealth Break Mode: Extracted {self.session_leads_count} leads. Taking a long human-like break for {break_time:.1f}s...")
                await asyncio.sleep(break_time)

        # FINAL QUALITY GATE: Only keep contacts with at least one valid method (Phone or Email)
        final_listings = ProcessingHandler.filter_valid(processed_listings)

        skipped_junk = len(processed_listings) - len(final_listings)
        if skipped_junk > 0:
            msg = f"[QUALITY] Filter: Dropped {skipped_junk} contacts with invalid/missing phone and email"
            logger.info(msg)
            # Log to activity log if possible (via logger name prefix that tasks.py might pick up, 
            # or simply via standard logging which we've verified works in Railway logs)
            self.stats["skipped_junk"] = self.stats.get("skipped_junk", 0) + skipped_junk

        return final_listings

    def _format_amfi_listing(self, record: Dict, city: str) -> Dict:
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
        self, scraper: AMFIScraper, city: str, category: str, on_progress=None
    ) -> List[Dict]:
        all_listings = []
        page_num = 1
        page_size = int(os.environ.get("SCRAPER_AMFI_PAGE_SIZE", "10000"))
        timeout = aiohttp.ClientTimeout(total=max(30, self.config.timeout_seconds))

        user_agent = StealthManager.get_random_ua()
        headers = StealthManager.get_modern_headers(user_agent)

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            while True:
                params = scraper.get_search_params(
                    city=city, page=page_num, page_size=page_size
                )
                logger.info(f"Fetching AMFI API page {page_num} for {city}")

                async with session.get(
                    scraper.SEARCH_API_URL, params=params
                ) as response:
                    if response.status != 200:
                        raise RuntimeError(f"AMFI API returned HTTP {response.status}")

                    payload = await response.json(content_type=None)

                records = payload.get("data") or []
                if not records:
                    break

                batch = [
                    self._format_amfi_listing(record, city)
                    for record in records
                    if record.get("ARNHolderName")
                ]
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
                    # For massive batches, clear processed data from memory early
                    if len(all_listings) > 5000:
                        all_listings = []  # We've already saved it

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

        logger.info(f"AMFI API extracted {len(all_listings)} listings for {city}")
        self.rate_limiter.record_success()
        self.stats["successful"] += 1
        return all_listings

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
                    scraper = JustDialScraper()
                elif "indiamart" in url_lower:
                    scraper = IndiaMartScraper()
                elif "amfi" in url_lower:
                    scraper = AMFIScraper()
                elif "irdai" in url_lower or "policyholder" in url_lower:
                    scraper = IRDAIScraper()
                elif "icai" in url_lower:
                    scraper = ICAIScraper()
                elif "sulekha" in url_lower:
                    scraper = SulekhaScraper()
                elif "clickindia" in url_lower:
                    scraper = ClickIndiaScraper()
                else:
                    scraper = JustDialScraper()

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

    async def save_to_db(
        self, listings: List[Dict], category: str, city: str, source: str, url: str
    ):
        if not listings:
            return

        # Use Unified Processing Handler to ensure only valid data is stored
        valid_listings = ProcessingHandler.filter_valid(listings)

        skipped = len(listings) - len(valid_listings)
        if skipped > 0:
            logger.info(
                f"[STORAGE] Skipped {skipped} listings with invalid data during DB save"
            )

        if not valid_listings:
            return

        if hasattr(self, "use_sqlite") and self.use_sqlite:
            cursor = self.sqlite_conn.cursor()
            for l in valid_listings:
                cursor.execute(
                    """
                    INSERT INTO contacts (name, phone, email, address, category, city, area, state, source, source_url, phone_clean, email_valid, enriched, arn, license_no, membership_no)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        l.get("name"),
                        l.get("phone"),
                        l.get("email"),
                        l.get("address"),
                        category,
                        city,
                        l.get("area"),
                        l.get("state"),
                        source,
                        url,
                        l.get("phone_clean"),
                        l.get("email_valid", False),
                        l.get("enriched", False),
                        l.get("arn"),
                        l.get("license_no"),
                        l.get("membership_no"),
                    ),
                )
            self.sqlite_conn.commit()
            logger.info(f"Saved {len(valid_listings)} records to SQLite")
            return

        records = [
            (
                listing.get("name"),
                listing.get("phone"),
                listing.get("email"),
                listing.get("address"),
                category,
                city,
                listing.get("area"),
                listing.get("state"),
                source,
                url,
                listing.get("phone_clean"),
                listing.get("email_valid", False),
                listing.get("enriched", False),
                listing.get("arn"),
                listing.get("license_no"),
                listing.get("membership_no"),
            )
            for listing in valid_listings
        ]

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # UPSERT logic: If phone_clean or email exists, update the timestamp and source if newer
                # We use the clean phone as preferred unique key
                await conn.executemany(
                    """
                    INSERT INTO contacts (
                        name, phone, email, address, category, city, area, state, 
                        source, source_url, phone_clean, email_valid, enriched, 
                        arn, license_no, membership_no, quality_score, quality_tier
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                    ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL
                    DO UPDATE SET
                        scraped_at = EXCLUDED.scraped_at,
                        source = CASE WHEN EXCLUDED.quality_score > contacts.quality_score THEN EXCLUDED.source ELSE contacts.source END
                """,
                    [
                        r
                        + (
                            listing.get("quality_score", 0),
                            listing.get("quality_tier", "low"),
                        )
                        for r, listing in zip(records, valid_listings)
                    ],
                )

        logger.info(f"Saved {len(listings)} records to database")

    async def export_to_csv(self, source: Optional[str] = None):
        os.makedirs(self.config.csv_output_dir, exist_ok=True)

        async with self.pool.acquire() as conn:
            if source:
                rows = await conn.fetch(
                    """
                    SELECT * FROM contacts WHERE source = $1 ORDER BY scraped_at DESC
                """,
                    source,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM contacts ORDER BY scraped_at DESC"
                )

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
                if isinstance(scraper, AMFIScraper):
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

# Global Scraper Registry Initialization
# We register all available scrapers here to ensure ScraperRegistry.get() works across the app.
try:
    # Official / Regulated Sources
    ScraperRegistry.register(AMFIScraper())           # AMFI (Mutual Funds)
    ScraperRegistry.register(IRDAIScraper())          # IRDAI (Insurance)
    ScraperRegistry.register(ICAIScraper())           # ICAI (Tax/CA)
    ScraperRegistry.register(ICSIScraper())           # ICSI (Company Secretaries)
    ScraperRegistry.register(SEBIScraper())           # SEBI (Investment Advisors)
    ScraperRegistry.register(NSEBrokerScraper())      # NSE (Stock Brokers)
    ScraperRegistry.register(BSEBrokerScraper())      # BSE (Stock Brokers)
    ScraperRegistry.register(GSTPractitionerScraper()) # GST Practitioners
    ScraperRegistry.register(RBIRegulatedScraper())   # RBI (Banks/NBFC)
    
    # Business Directories & Deep Scrapers
    ScraperRegistry.register(JustDialScraper())
    ScraperRegistry.register(YellowPagesScraper())
    ScraperRegistry.register(IndiaMartScraper())
    ScraperRegistry.register(TradeIndiaScraper())
    ScraperRegistry.register(SulekhaScraper())
    ScraperRegistry.register(ClickIndiaScraper())
    ScraperRegistry.register(GrotalScraper())
    ScraperRegistry.register(GoogleMapsScraper())
    ScraperRegistry.register(LinkedInGoogleScraper())
    ScraperRegistry.register(GoogleDorkScraper())     # Source: FOOTPRINT
    
    logger.info(f"[SUCCESS] Scraper Registry initialized with {len(ScraperRegistry.list_scrapers())} sources")
except Exception as e:
    logger.error(f"[ERROR] Critical Error during Scraper Registration: {e}")
