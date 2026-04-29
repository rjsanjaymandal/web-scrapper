from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Type
import logging
import re

try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

logger = logging.getLogger(__name__)

# Mock Page for type hinting when playwright is not used
Page = dict

class BaseScraper(ABC):
    @abstractmethod
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        """Construct the search URL for a given city and category."""
        pass

    @abstractmethod
    async def extract_listings(
        self,
        page: Page,
        city: str = None,
        category: str = None,
        html_content: str = None,
    ) -> List[Dict]:
        """Extract contact listings from the current page."""
        pass

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r"[^\d]", "", str(phone))
        if len(digits) == 12 and digits.startswith("91"):
            return digits[-10:]
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None

    @property
    def force_http1(self) -> bool:
        """Whether to force HTTP/1.1 for this source."""
        return False

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of the data source (e.g., 'JustDial')."""
        pass

    def extract_raw_fallback(self, html_content: str, city: str, category: str) -> List[Dict]:
        """
        Enterprise feature: If DOM selectors fail, use raw regex over HTML to catch orphaned leads.
        """
        listings = []
        if not html_content:
            return listings

        clean_text = html_content
        if HTMLParser:
            try:
                tree = HTMLParser(html_content)
                tree.strip_tags(['script', 'style', 'path', 'svg', 'noscript', 'meta', 'link'])
                clean_text = tree.text(separator=' ')
            except Exception as e:
                logger.warning(f"Selectolax parsing failed: {e}. Falling back to raw HTML regex.")

        email_pattern = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
        phone_pattern = re.compile(r"(\+91[-.\s]??\d{10}|\b\d{10}\b|\b0\d{10,11}\b)")
        
        emails = list(set(email_pattern.findall(clean_text)))
        phones = list(set(phone_pattern.findall(clean_text)))
        
        for email in emails:
            listings.append({
                "name": "Raw Extracted",
                "email": email,
                "phone": None,
                "city": city,
                "category": category,
            })
        
        for phone in phones:
            listings.append({
                "name": "Raw Extracted",
                "email": None,
                "phone": phone,
                "city": city,
                "category": category,
            })
            
        if listings:
            logger.info(f"Fallback Regex Extracted {len(emails)} emails and {len(phones)} phones.")
            
        return listings


class ScraperRegistry:
    """Central registry for all scrapers."""

    _scrapers: Dict[str, BaseScraper] = {}

    @classmethod
    def register(cls, scraper_or_name, scraper_class: Optional[Type[BaseScraper]] = None):
        """
        Flexible registration:
        - register(instance)
        - register(class)
        - register("NAME", class)
        """
        if scraper_class:
            name = scraper_or_name.upper()
            instance = scraper_class()
        else:
            if isinstance(scraper_or_name, type):
                instance = scraper_or_name()
            else:
                instance = scraper_or_name
            
            try:
                name = instance.source_name.upper()
            except AttributeError:
                # Fallback for classes that don't have source_name as class attribute
                name = instance.__class__.__name__.replace("Scraper", "").upper()
        
        cls._scrapers[name] = instance
        logger.info(f"Registered scraper: {name}")

    @classmethod
    def get(cls, name: str) -> Optional[BaseScraper]:
        return cls._scrapers.get(name.upper())

    @classmethod
    def list_scrapers(cls) -> List[str]:
        return list(cls._scrapers.keys())

    @classmethod
    def get_all_sources_for_category(cls, category: str) -> List[str]:
        """Get ALL sources for a category (for comprehensive scraping)."""
        cat_lower = category.lower()
        sources = []

        if "mutual" in cat_lower:
            sources = ["AMFI", "FOOTPRINT"]
        elif "insurance" in cat_lower:
            sources = ["IRDAI", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        elif "advisor" in cat_lower or "adviser" in cat_lower or "sebi" in cat_lower:
            sources = ["SEBI", "YELLOWPAGES", "FOOTPRINT"]
        elif "tax" in cat_lower:
            sources = ["ICAI", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        elif "chartered" in cat_lower or "ca" in cat_lower:
            sources = ["ICAI", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        elif "company" in cat_lower or "secretary" in cat_lower:
            sources = ["ICSI", "YELLOWPAGES", "FOOTPRINT"]
        elif "stock" in cat_lower or "broker" in cat_lower:
            sources = ["NSE", "BSE", "YELLOWPAGES", "FOOTPRINT"]
        elif "gst" in cat_lower:
            sources = ["GST", "YELLOWPAGES", "FOOTPRINT"]
        elif "rbi" in cat_lower or "bank" in cat_lower or "nbfc" in cat_lower:
            sources = ["RBI", "YELLOWPAGES", "FOOTPRINT"]
        elif "financial" in cat_lower or "wealth" in cat_lower or "investment" in cat_lower:
            sources = ["AMFI", "SEBI", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        elif "insolvency" in cat_lower:
            sources = ["IBBI", "FOOTPRINT"]
        elif "lawyer" in cat_lower or "advocate" in cat_lower or "bar" in cat_lower:
            sources = ["BAR_COUNCIL", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        else:
            sources = ["YELLOWPAGES", "JUSTDIAL", "INDIAMART", "TRADEINDIA", "EXPORTERSINDIA", "ASKLAILA", "VYKARI", "SULEKHA", "GROTAL", "SITEMAP", "FOOTPRINT"]

        return sources if sources else ["YELLOWPAGES", "EXPORTERSINDIA", "SITEMAP", "FOOTPRINT"]
