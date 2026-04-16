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

    # Register enhanced scrapers
    ScraperRegistry.register(SulekhaScraper())
    ScraperRegistry.register(ClickIndiaScraper())
    ScraperRegistry.register(GrotalScraper())
    ScraperRegistry.register(SEBIScraper())
    ScraperRegistry.register(NSEScraper())
    ScraperRegistry.register(GoogleMapsScraper())
    ScraperRegistry.register(LinkedInGoogleScraper())
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


def load_config() -> Config:
    config_path = Path("config.yaml")
    data = {}
    if config_path.exists():
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}

    scraper_cfg = data.get("scraper", {})
    db_cfg = data.get("database", {})
    proxy_cfg = data.get("proxy", {})

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
            os.environ.get("SCRAPER_DELAY_MIN", scraper_cfg.get("request_delay_min", 2))
        ),
        request_delay_max=int(
            os.environ.get("SCRAPER_DELAY_MAX", scraper_cfg.get("request_delay_max", 5))
        ),
        max_retries=int(
            os.environ.get("SCRAPER_MAX_RETRIES", scraper_cfg.get("max_retries", 3))
        ),
        timeout_seconds=int(
            os.environ.get("SCRAPER_TIMEOUT", scraper_cfg.get("timeout_seconds", 30))
        ),
        headless=os.environ.get(
            "SCRAPER_HEADLESS", str(scraper_cfg.get("headless", True))
        ).lower()
        == "true",
        test_mode=os.environ.get(
            "SCRAPER_TEST_MODE", str(scraper_cfg.get("test_mode", False))
        ).lower()
        == "true",
        export_csv=os.environ.get(
            "SCRAPER_EXPORT_CSV", str(scraper_cfg.get("export_csv", True))
        ).lower()
        == "true",
        csv_output_dir=os.environ.get(
            "SCRAPER_EXPORT_DIR", scraper_cfg.get("csv_output_dir", "exports")
        ),
        enable_email_extraction=os.environ.get(
            "SCRAPER_ENABLE_EMAIL",
            str(scraper_cfg.get("enable_email_extraction", True)),
        ).lower()
        == "true",
        enable_sitemap=os.environ.get(
            "SCRAPER_ENABLE_SITEMAP", str(scraper_cfg.get("enable_sitemap", False))
        ).lower()
        == "true",
        enable_deduplication=os.environ.get(
            "SCRAPER_ENABLE_DEDUPE", str(scraper_cfg.get("enable_deduplication", True))
        ).lower()
        == "true",
        enable_email_verify=os.environ.get(
            "SCRAPER_ENABLE_EMAIL_VERIFY",
            str(scraper_cfg.get("enable_email_verify", False)),
        ).lower()
        == "true",
        enable_enrichment=os.environ.get(
            "SCRAPER_ENABLE_ENRICH", str(scraper_cfg.get("enable_enrichment", False))
        ).lower()
        == "true",
        scheduler_enabled=os.environ.get(
            "SCRAPER_SCHEDULER_ENABLED",
            str(scraper_cfg.get("scheduler_enabled", False)),
        ).lower()
        == "true",
        scheduler_interval_hours=int(
            os.environ.get(
                "SCRAPER_SCHEDULER_INTERVAL",
                scraper_cfg.get("scheduler_interval_hours", 24),
            )
        ),
        max_pages=int(
            os.environ.get(
                "SCRAPER_MAX_PAGES", scraper_cfg.get("max_pages_per_source", 3)
            )
        ),
        dashboard_page_size=int(
            os.environ.get(
                "DASHBOARD_PAGE_SIZE", scraper_cfg.get("dashboard_page_size", 50)
            )
        ),
        categories=data.get("categories", []),
        cities=data.get("cities", []),
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
        try:
            icons = await card.query_selector_all(
                'span[class*="mobilesv"], span[class*="icon"], [class*="phone"]'
            )
            if icons:
                jd_map = {
                    "icon-ji": "9",
                    "icon-dc": "0",
                    "icon-fe": "1",
                    "icon-hg": "2",
                    "icon-ba": "3",
                    "icon-lk": "4",
                    "icon-nm": "5",
                    "icon-op": "6",
                    "icon-rq": "7",
                    "icon-ts": "8",
                    "icon-acb": "0",
                    "icon-yz": "1",
                    "icon-wx": "2",
                    "icon-vu": "3",
                    "icon-ts": "4",
                    "icon-rq": "5",
                    "icon-pon": "6",
                    "icon-mlk": "7",
                    "icon-jih": "8",
                    "icon-gfed": "9",
                    "mcl-": "",
                }
                phone_digits = []
                for icon in icons:
                    class_attr = await icon.get_attribute("class") or ""
                    for icon_class, digit in jd_map.items():
                        if icon_class in class_attr:
                            if digit:
                                phone_digits.append(digit)
                            break
                if len(phone_digits) >= 8:
                    return "".join(phone_digits)

            text = await self._get_text(
                card, '.store-phone, .phone, [class*="contact"], a[href*="tel"]'
            )
            if text:
                return self._clean_phone(text)

            all_text = await card.inner_text() if card else ""
            phone_match = re.search(r"(\d{8,12})", all_text)
            if phone_match:
                return self._clean_phone(phone_match.group(1))
        except Exception as e:
            logger.debug(f"Phone extraction error: {e}")
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
            await asyncio.sleep(3)

            page_title = await page.title()
            page_url = page.url
            logger.info(f"JustDial page title: {page_title}")
            logger.info(f"JustDial page URL: {page_url}")

            page_content = await page.content()
            logger.info(f"Page content length: {len(page_content)}")

            card_selectors = [
                ".store-list .store-info",
                ".store-list .results",
                ".store-info",
                ".results .store-info",
                '[class*="store"]',
                ".listing-card",
                ".business-card",
                "li.store-data",
                ".srch-result",
                ".clg-listing",
                ".search-result",
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
                        ".store-name",
                        ".name",
                        "h2",
                        "h3",
                        ".business-name",
                        '[class*="name"]',
                        "a.store-name",
                    ]
                    name = None
                    for sel in name_selectors:
                        name = await self._get_text(card, sel)
                        if name:
                            break

                    phone = await self._extract_phone(card)

                    addr_selectors = [
                        ".store-address",
                        ".address",
                        ".addr",
                        '[class*="address"]',
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

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return "https://www.amfiindia.com/locate-distributor"

    def get_search_params(self, city: str, page: int = 1, page_size: int = 100) -> Dict:
        return {
            "strOpt": "ALL",
            "city": city,
            "page": page,
            "pageSize": page_size,
        }

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
        logger.info(f"AMFI: Starting extraction for city={city}, category={category}")

        try:
            # Log page info
            page_title = await page.title()
            page_url = page.url
            logger.info(f"AMFI: Page title: {page_title}")
            logger.info(f"AMFI: Page URL: {page_url}")

            # Step 1: Click "Filter Location" to expand location filters
            filter_location_selectors = [
                'span:has-text("Filter Location")',
                'button:has-text("Filter Location")',
                'a:has-text("Filter Location")',
                '[class*="filter-location"]',
                "#filterLocation",
                '[onclick*="Filter"]',
            ]

            filter_clicked = False
            for sel in filter_location_selectors:
                try:
                    filter_elem = await page.query_selector(sel)
                    if filter_elem:
                        await filter_elem.click()
                        logger.info(f"AMFI: Clicked Filter Location: {sel}")
                        filter_clicked = True
                        await asyncio.sleep(2)
                        break
                except Exception as e:
                    logger.debug(f"AMFI: Could not click {sel}: {e}")
                    continue

            if not filter_clicked:
                logger.warning("AMFI: Could not click Filter Location")

            # Take screenshot after clicking filter
            await page.screenshot(path="amfi_filter.png")

            # Step 2: Now find and select city from the expanded filter
            city_input_selectors = [
                'input[placeholder*="City"]',
                'input[id*="city"]',
                'input[id*="City"]',
                'input[name*="city"]',
                'input[id*="ddlCity"]',
                'input[id*="loc"]',
            ]

            city_filled = False
            for sel in city_input_selectors:
                try:
                    input_elem = await page.query_selector(sel)
                    if input_elem:
                        await input_elem.click()
                        await input_elem.fill("")
                        await asyncio.sleep(0.3)

                        # Type city name
                        for char in city or "Delhi":
                            await input_elem.type(char, delay=50)

                        logger.info(f"AMFI: Typed city: {city or 'Delhi'}")
                        city_filled = True
                        await asyncio.sleep(1)
                        break
                except Exception as e:
                    logger.debug(f"AMFI: Error with {sel}: {e}")
                    continue

            # Step 3: Wait for suggestions and select
            if city_filled:
                suggestion_selectors = [
                    'li[class*="suggestion"]',
                    'li[class*="ui-menu"]',
                    '[class*="autocomplete"] li',
                    "ul li",
                    'li:has-text("' + (city or "Delhi") + '")',
                ]

                for sug_sel in suggestion_selectors:
                    try:
                        suggestions = await page.query_selector_all(sug_sel)
                        if suggestions and len(suggestions) > 0:
                            logger.info(f"AMFI: Found {len(suggestions)} suggestions")
                            # Click first suggestion
                            await suggestions[0].click()
                            logger.info("AMFI: Clicked first suggestion")
                            break
                    except Exception:
                        continue

                await asyncio.sleep(1)

            # Step 4: Click Search/Submit button
            search_button_selectors = [
                'button:has-text("Search")',
                'input[type="submit"]',
                'button[type="submit"]',
                'input[value*="Search"]',
                "#search, #btnSearch",
                'button:has-text("Submit")',
            ]

            for sel in search_button_selectors:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        logger.info(f"AMFI: Clicked search button: {sel}")
                        break
                except Exception:
                    continue

            # Wait for results
            logger.info("AMFI: Waiting for results to load...")
            await asyncio.sleep(3)
            await page.wait_for_load_state("networkidle", timeout=15000)

            # Take screenshot of results
            await page.screenshot(path="amfi_results.png")
            logger.info("AMFI: Screenshot saved: amfi_results.png")

            # Log current page state
            body_text = await page.inner_text("body")
            logger.info(f"AMFI: Page text preview (first 500 chars): {body_text[:500]}")

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
                '[class*="table"] tbody tr',
                ".table-responsive table tr",
            ]

            rows = []
            active_selector = None
            for selector in row_selectors:
                rows = await page.query_selector_all(selector)
                if rows:
                    active_selector = selector
                    logger.info(
                        f"AMFI: Found {len(rows)} rows with selector: {selector}"
                    )
                    break

            if not rows:
                # ... (JS fallback logic remains)

                logger.warning("AMFI: No rows found with standard selectors!")

                # Try using JavaScript to get all content
                try:
                    js_result = await page.evaluate("""() => {
                        // Get all text content
                        const body = document.body;
                        const text = body.innerText;
                        
                        // Get all table data
                        const tables = document.querySelectorAll('table');
                        let tableData = [];
                        tables.forEach((table, i) => {
                            tableData.push({
                                index: i,
                                html: table.outerHTML.substring(0, 1000)
                            });
                        });
                        
                        // Get all divs with data
                        const divs = document.querySelectorAll('div[class*="row"], div[class*="data"], div[class*="result"]');
                        let divData = [];
                        divs.forEach((div, i) => {
                            if (i < 5) {
                                divData.push({
                                    class: div.className,
                                    text: div.innerText.substring(0, 200)
                                });
                            }
                        });
                        
                        return {
                            textLength: text.length,
                            textPreview: text.substring(0, 1000),
                            tableCount: tables.length,
                            tables: tableData,
                            divCount: divs.length,
                            divs: divData
                        };
                    }""")

                    logger.info(
                        f"AMFI: JS Result - text length: {js_result['textLength']}"
                    )
                    logger.info(
                        f"AMFI: JS Result - text preview: {js_result['textPreview'][:500]}"
                    )
                    logger.info(
                        f"AMFI: JS Result - table count: {js_result['tableCount']}"
                    )
                    logger.info(f"AMFI: JS Result - div count: {js_result['divCount']}")

                    if js_result["tables"]:
                        for t in js_result["tables"]:
                            logger.info(
                                f"AMFI: JS Table {t['index']}: {t['html'][:300]}"
                            )

                    if js_result["divs"]:
                        for d in js_result["divs"]:
                            logger.info(f"AMFI: JS Div {d['class']}: {d['text'][:100]}")

                    # Try to parse text content for distributor data
                    text = js_result["textPreview"]

                    # Look for ARN patterns in the text
                    arn_pattern = re.compile(
                        r"(\d{6,})\s+([A-Z][A-Z\s]+)\s+(\d{2}/\d{2}/\d{4})",
                        re.MULTILINE,
                    )
                    matches = arn_pattern.findall(text)

                    if matches:
                        logger.info(
                            f"AMFI: Found {len(matches)} potential distributor matches via regex"
                        )
                        for match in matches[:10]:
                            arn, name, date = match
                            if len(name.strip()) > 3:
                                listings.append(
                                    {
                                        "name": name.strip(),
                                        "arn": arn.strip(),
                                        "city": city,
                                        "state": None,
                                        "phone": None,
                                        "address": None,
                                        "area": None,
                                        "detail_url": None,
                                    }
                                )
                        logger.info(
                            f"AMFI: Extracted {len(listings)} listings via regex"
                        )

                except Exception as js_err:
                    logger.error(f"AMFI: JavaScript evaluation error: {js_err}")

                return listings

            # Extract data from rows
            for row in rows:
                try:
                    # In MUI grid, children are divs. In tables, children are tds.
                    cols = await row.query_selector_all(
                        '> div, td, .col, [class*="cell"]'
                    )

                    if not cols or len(cols) < 2:
                        continue

                    # If it's the new MUI grid (usually ~11 columns)
                    if len(cols) >= 10 and "MuiBox-root" in active_selector:
                        # Based on research: ARN=1, Name=2, City=7 (1-based, so 0, 1, 6)
                        arn_result = await cols[0].inner_text()
                        name = await cols[1].inner_text()
                        city_result = (
                            await cols[6].inner_text() if len(cols) > 6 else city
                        )
                        state_result = None  # Might be in there too
                    else:
                        # Standard table fallback
                        name = await cols[0].inner_text()
                        arn_result = (
                            await cols[1].inner_text() if len(cols) > 1 else None
                        )
                        city_result = (
                            await cols[2].inner_text() if len(cols) > 2 else city
                        )
                        state_result = (
                            await cols[3].inner_text() if len(cols) > 3 else None
                        )

                    if name and arn_result and len(name.strip()) > 1:
                        listings.append(
                            {
                                "name": name.strip(),
                                "arn": arn_result.strip(),
                                "city": city_result.strip() if city_result else city,
                                "state": state_result.strip() if state_result else None,
                                "phone": None,
                                "address": None,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                except Exception as e:
                    logger.debug(f"AMFI: Row parse error: {e}")
                    continue

            logger.info(f"AMFI: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"AMFI: Extraction error: {e}")
            import traceback

            logger.error(f"AMFI: Traceback: {traceback.format_exc()}")

        return listings

    async def fetch_with_post(
        self, session: aiohttp.ClientSession, city: str = None, state: str = None
    ) -> List[Dict]:
        listings = []
        try:
            payload = {}
            if city:
                payload["city"] = city
            if state:
                payload["state"] = state

            async with session.post(
                self.ARN_BASE_URL, data=payload, timeout=30
            ) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    logger.debug(f"AMFI response length: {len(html)}")
        except Exception as e:
            logger.warning(f"AMFI POST error: {e}")
        return listings

    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


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
            # Log page info
            page_title = await page.title()
            page_url = page.url
            logger.info(f"IRDAI: Page title: {page_title}")
            logger.info(f"IRDAI: Page URL: {page_url}")

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
    """Scraper for ICAI CA directory data (caconnect.icai.org)"""

    source_name = "ICAI"

    MEMBER_SEARCH_URL = "https://caconnect.icai.org/city-wise-list"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"{self.MEMBER_SEARCH_URL}/{city.title()}"

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
    """Scraper for ICSI (Institute of Company Secretaries of India) directory"""

    source_name = "ICSI"

    MEMBER_SEARCH_URL = "https://www.icsi.edu/member/search"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"{self.MEMBER_SEARCH_URL}?city={city.lower()}"

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
    """Scraper for BSE (Bombay Stock Exchange) registered brokers"""

    source_name = "BSE"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return "https://www.bseindia.com/corporates/mktdata.html"

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
        logger.info(f"BSE: Starting extraction for city={city}")

        try:
            await page.wait_for_selector("body", timeout=15000)
            page_url = page.url
            logger.info(f"BSE: Page URL: {page_url}")

            body_text = await page.inner_text("body")

            name_pattern = re.compile(
                r"([A-Z][A-Z\s]+(?:Broking|Securities|Pvt|Ltd|Stock)[^\d\n]{2,50})",
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

            logger.info(f"BSE: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"BSE: Extraction error: {e}")

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
        cat_slug = category.lower().replace(" ", "-")
        city_slug = city.lower().replace(" ", "-")
        if page == 1:
            return f"{self.BASE_URL}/{city_slug}/{cat_slug}"
        return f"{self.BASE_URL}/{city_slug}/{cat_slug}?page={page}"

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

            card_selectors = [
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

            if not cards:
                logger.warning("YellowPages: No cards found, trying text extraction")
                body_text = await page.inner_text("body")
                lines = [
                    l.strip() for l in body_text.split("\n") if l.strip() and len(l) > 5
                ]

                for line in lines[:30]:
                    if len(line) > 10 and len(line) < 150:
                        listings.append(
                            {
                                "name": line[:100],
                                "phone": None,
                                "email": None,
                                "address": None,
                                "city": city,
                                "area": None,
                                "detail_url": None,
                            }
                        )
                return listings

            for card in cards:
                try:
                    name = await self._get_text(
                        card, "h2, h3, .business-name, .title, .name"
                    )
                    phone = await self._get_text(
                        card, ".phone, .contact-phone, [class*='phone']"
                    )
                    address = await self._get_text(
                        card, ".address, .location, .contact-addr"
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
        cat_slug = category.lower().replace(" ", "-")
        city_slug = city.lower().replace(" ", "-")
        return f"{self.BASE_URL}/search/{cat_slug}-{city_slug}.html?page={page}"

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

            cards = await page.query_selector_all(
                ".company-list .company-item, .search-result .result-item, [class*='company']"
            )

            if not cards:
                body_text = await page.inner_text("body")
                lines = [
                    l.strip()
                    for l in body_text.split("\n")
                    if l.strip() and 5 < len(l) < 100
                ]
                for line in lines[:20]:
                    listings.append(
                        {
                            "name": line[:100],
                            "phone": None,
                            "email": None,
                            "address": None,
                            "city": city,
                            "area": None,
                            "detail_url": None,
                        }
                    )
                return listings

            for card in cards:
                try:
                    name = await self._get_text(
                        card, "h3, h4, .company-name, .company-title"
                    )
                    phone = await self._get_text(
                        card, ".phone, .contact, [class*='mobile']"
                    )
                    address = await self._get_text(card, ".address, .location")

                    if name and len(name.strip()) > 2:
                        listings.append(
                            {
                                "name": name.strip()[:150],
                                "phone": self._clean_phone(phone) if phone else None,
                                "email": None,
                                "address": address.strip() if address else None,
                                "city": city,
                                "area": None,
                                "detail_url": await self.get_detail_url(card),
                            }
                        )
                except:
                    continue

            logger.info(f"TradeIndia: Total listings extracted: {len(listings)}")

        except Exception as e:
            logger.error(f"TradeIndia: Extraction error: {e}")

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
        cat_slug = category.lower().replace(" ", "-")
        city_slug = city.lower().replace(" ", "-")
        return f"{self.BASE_URL}/prodir/{cat_slug}-in-{city_slug}/?pn={page}"

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

            cards = await page.query_selector_all(
                ".prod-list .prod-item, .seller-card, [class*='seller']"
            )

            if not cards:
                return listings

            for card in cards:
                try:
                    name = await self._get_text(card, ".company-name, .prod-name, h3")
                    phone = await self._get_text(
                        card, ".prod-phn, .contact, [class*='phone']"
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

                # Get dynamic User-Agent and modern headers
                user_agent = StealthManager.get_random_ua()
                extra_headers = StealthManager.get_modern_headers(user_agent)

                context_kwargs = {
                    "user_agent": user_agent,
                    "extra_http_headers": extra_headers,
                    "viewport": {"width": 1920, "height": 1080},
                    "ignore_https_errors": True,
                }
                if proxy_dict:
                    context_kwargs["proxy"] = proxy_dict

                self.context = await self.browser.new_context(**context_kwargs)

                # Apply advanced stealth patches
                await StealthManager.apply_stealth(self.context)

                self.page = await self.context.new_page()

                logger.info(
                    f"Browser initialized with dynamic UA: {user_agent[:40]}..."
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

        # FINAL QUALITY GATE: Only keep contacts with at least one valid method (Phone or Email)
        final_listings = ProcessingHandler.filter_valid(processed_listings)

        skipped_junk = len(processed_listings) - len(final_listings)
        if skipped_junk > 0:
            msg = f"🛡️ Quality Filter: Dropped {skipped_junk} contacts with invalid/missing phone and email"
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

                    page_text = await self.page.inner_text("body")
                    logger.info(
                        f"Page text preview (first 500 chars): {page_text[:500]}"
                    )
                    page_text_lower = page_text.lower()

                    if (
                        "captcha" in page_text_lower
                        or "verify" in page_text_lower
                        or "robot" in page_text_lower
                    ):
                        logger.warning("CAPTCHA or bot detection detected!")
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
                        logger.info(f"💾 Raw HTML saved: {raw_path}")

                    # Anti-Detection: Standard randomized jitter delay (12:56)
                    jitter = random.uniform(
                        self.config.request_delay_min, self.config.request_delay_max
                    )
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
        except Exception as e:
            logger.warning(f"Extraction error: {e}")
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
                f"🛡️ Skipped {skipped} listings with invalid data during DB save"
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

# Register scrapers
ScraperRegistry.register(JustDialScraper())
ScraperRegistry.register(IndiaMartScraper())
ScraperRegistry.register(ICICIScraper())
ScraperRegistry.register(AMFIScraper())
ScraperRegistry.register(ICSIScraper())
ScraperRegistry.register(SEBIScraper())
ScraperRegistry.register(NSEBrokerScraper())
ScraperRegistry.register(BSEBrokerScraper())
ScraperRegistry.register(GSTPractitionerScraper())
ScraperRegistry.register(RBIRegulatedScraper())
ScraperRegistry.register(YellowPagesScraper())
ScraperRegistry.register(TradeIndiaScraper())
