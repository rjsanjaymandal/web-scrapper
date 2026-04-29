from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Type
# from playwright.async_api import Page
Page = dict
import logging
import re
try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

logger = logging.getLogger(__name__)


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
        # Remove all non-numeric characters
        digits = re.sub(r"[^\d]", "", str(phone))
        # Handle Indian numbers: if 12 digits starting with 91, take last 10
        if len(digits) == 12 and digits.startswith("91"):
            return digits[-10:]
        # Standard 10 digits
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

        # Phase 3: High-Speed C-Bindings Parsing (selectolax)
        clean_text = html_content
        if HTMLParser:
            try:
                tree = HTMLParser(html_content)
                # Strip out noisy tags that cause false positive regex matches
                tree.strip_tags(['script', 'style', 'path', 'svg', 'noscript', 'meta', 'link'])
                clean_text = tree.text(separator=' ')
            except Exception as e:
                logger.warning(f"Selectolax parsing failed: {e}. Falling back to raw HTML regex.")

        # Match generic emails
        email_pattern = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
        # Match Indian mobile patterns, landlines loosely
        phone_pattern = re.compile(r"(\+91[-.\s]??\d{10}|\b\d{10}\b|\b0\d{10,11}\b)")
        
        emails = list(set(email_pattern.findall(clean_text)))
        phones = list(set(phone_pattern.findall(clean_text)))
        
        # We can't link an email to a phone easily when scraping raw, 
        # so we inject them individually as "Raw Extract" names.
        for email in emails:
            listings.append({
                "name": "Raw Extracted",
                "email": email,
                "phone": None,
                "city": city,
                "category": category,
            })
        
        # Don't duplicate if we have more phones than emails, append remaining
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
    def register(cls, scraper_instance: BaseScraper):
        name = scraper_instance.source_name.upper()
        cls._scrapers[name] = scraper_instance
        logger.info(f"Registered scraper: {name}")

    @classmethod
    def get(cls, name: str) -> Optional[BaseScraper]:
        return cls._scrapers.get(name.upper())

    @classmethod
    def list_scrapers(self) -> List[str]:
        return list(self._scrapers.keys())

    @classmethod
    def get_source_for_category(cls, category: str) -> str:
        """Map a category to the most reliable source (single fallback)."""
        cat_lower = category.lower()

        # Professional/Person Searches
        if (
            "person" in cat_lower
            or "profile" in cat_lower
            or "professional" in cat_lower
        ):
            return "LINKEDIN"

        # Specialized Financial Sources
        if "mutual" in cat_lower:
            return "AMFI"
        elif "insurance" in cat_lower:
            return "IRDAI"
        elif "advisor" in cat_lower or "adviser" in cat_lower or "sebi" in cat_lower:
            return "SEBI"
        elif "tax" in cat_lower or "chartered" in cat_lower or "ca" in cat_lower:
            return "ICAI"
        elif "company" in cat_lower or "secretary" in cat_lower:
            return "ICSI"
        elif "stock" in cat_lower or "broker" in cat_lower or "authorized" in cat_lower:
            return "NSE"
        elif "gst" in cat_lower:
            return "GST"
        elif "rbi" in cat_lower or "bank" in cat_lower or "nbfc" in cat_lower:
            return "RBI"

        # Business/Local Directories - prioritize high-volume sources
        elif "map" in cat_lower or "location" in cat_lower:
            return "YELLOWPAGES"
        elif "business" in cat_lower or "shop" in cat_lower or "factory" in cat_lower:
            return "YELLOWPAGES"
        elif "local" in cat_lower:
            return "YELLOWPAGES"

        # Default to high-volume business directory
        return "YELLOWPAGES"

    @classmethod
    def get_all_sources_for_category(cls, category: str) -> List[str]:
        """Get ALL sources for a category (for comprehensive scraping)."""
        cat_lower = category.lower()
        sources = []

        # Financial Services - multiple sources
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
        elif (
            "financial" in cat_lower
            or "wealth" in cat_lower
            or "investment" in cat_lower
        ):
            sources = ["AMFI", "SEBI", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]
        
        elif "insolvency" in cat_lower:
            sources = ["IBBI", "FOOTPRINT"]
        
        elif "lawyer" in cat_lower or "advocate" in cat_lower or "bar" in cat_lower:
            sources = ["BAR_COUNCIL", "YELLOWPAGES", "JUSTDIAL", "FOOTPRINT"]

        # Business/Professional - multiple directories
        else:
            sources = ["YELLOWPAGES", "JUSTDIAL", "INDIAMART", "TRADEINDIA", "EXPORTERSINDIA", "ASKLAILA", "VYKARI", "SULEKHA", "GROTAL", "SITEMAP", "FOOTPRINT"]

        return sources if sources else ["YELLOWPAGES", "EXPORTERSINDIA", "SITEMAP", "FOOTPRINT"]
