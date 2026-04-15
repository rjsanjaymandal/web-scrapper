from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from playwright.async_api import Page
import logging

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    @abstractmethod
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        """Construct the search URL for a given city and category."""
        pass

    @abstractmethod
    async def extract_listings(
        self, page: Page, city: str = None, category: str = None
    ) -> List[Dict]:
        """Extract contact listings from the current page."""
        pass

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of the data source (e.g., 'JustDial')."""
        pass


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
        """Map a category to the most reliable source."""
        cat_lower = category.lower()
        if "mutual" in cat_lower:
            return "AMFI"
        elif "insurance" in cat_lower:
            return "IRDAI"
        elif "tax" in cat_lower or "chartered" in cat_lower:
            return "ICAI"
        elif "company" in cat_lower or "secretary" in cat_lower:
            return "ICSI"
        elif "stock" in cat_lower or "broker" in cat_lower:
            return "NSE"
        elif "sebi" in cat_lower:
            return "SEBI"
        elif "gst" in cat_lower:
            return "GST"
        elif "rbi" in cat_lower or "bank" in cat_lower or "nbfc" in cat_lower:
            return "RBI"
        elif "business" in cat_lower:
            return "INDIAMART"
        return "JUSTDIAL"
