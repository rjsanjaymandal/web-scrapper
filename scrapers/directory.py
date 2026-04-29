import logging
import re
import random
import asyncio
from typing import List, Dict, Optional
from scrapers.base import BaseScraper, ScraperRegistry

logger = logging.getLogger(__name__)

class SitemapScraper(BaseScraper):
    """
    High-speed source: extracts leads directly from XML sitemaps 
    to avoid WAF blocks and browser overhead.
    """
    source_name = "SITEMAP"
    
    SITEMAP_TARGETS = [
        "https://www.exportersindia.com/sitemap.xml",
        "https://www.asklaila.com/sitemap.xml",
        "https://www.tradeindia.com/sitemap.xml",
        "https://www.justdial.com/sitemap.xml",
        "https://www.indiamart.com/sitemap.xml"
    ]

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return random.choice(self.SITEMAP_TARGETS)

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        # For sitemaps, we usually use OfficialAPIHandlers.handle_sitemap
        # but if we have html_content (e.g. from a fetch), we parse it raw
        if html_content:
            return self.extract_raw_fallback(html_content, city, category)
        return []

class GoogleMapsScraper(BaseScraper):
    """Deep scraper for Google Maps Business (GMB) listings."""
    source_name = "GMB"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        query = f"{category} in {city}".replace(' ', '+')
        return f"https://www.google.com/maps/search/{query}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        # GMB usually requires live browser for scrolling
        if html_content:
            return self.extract_raw_fallback(html_content, city, category)
        return []

class LinkedInGoogleScraper(BaseScraper):
    """Scrapes LinkedIn leads via Google Search to avoid account bans."""
    source_name = "LINKEDIN"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        query = f'site:linkedin.com/in/ "{category}" "{city}"'
        return f"https://www.google.com/search?q={query.replace(' ', '+')}&start={(page-1)*10}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            return self.extract_raw_fallback(html_content, city, category)
        return []

class GoogleDorkScraper(BaseScraper):
    """Uses search engine dorks for high-speed lead discovery."""
    source_name = "DORK"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        dorks = [
            f'"{category}" "{city}" "phone" "@gmail.com"',
            f'"{category}" "{city}" "contact" "91"',
            f'intitle:"{category}" intext:"{city}"'
        ]
        query = random.choice(dorks)
        return f"https://www.google.com/search?q={query.replace(' ', '+')}&start={(page-1)*10}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            return self.extract_raw_fallback(html_content, city, category)
        return []

class YellowPagesIndiaScraper(BaseScraper):
    """Less security-heavy directory: Yellow Pages India."""
    source_name = "YELLOWPAGES"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # Format: https://www.yellowpages.in/search/city/category
        clean_city = city.replace(" ", "-").lower()
        clean_cat = category.replace(" ", "-").lower()
        if page > 1:
            return f"https://www.yellowpages.in/search/{clean_city}/{clean_cat}?page={page}"
        return f"https://www.yellowpages.in/search/{clean_city}/{clean_cat}"

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            from bs4 import BeautifulSoup
            content = html_content or await (page.content() if hasattr(page, 'content') else "")
            if not content: return []
            
            soup = BeautifulSoup(content, 'lxml')
            cards = soup.select('.business-card, .listing-item')
            
            for card in cards:
                name_el = card.select_one('.business-name, h3')
                phone_el = card.select_one('.phone-number, .contact-btn')
                
                if name_el:
                    listings.append({
                        'name': name_el.get_text(strip=True),
                        'phone': phone_el.get_text(strip=True) if phone_el else None,
                        'category': category,
                        'city': city,
                        'source': self.source_name
                    })
        except Exception as e:
            logger.debug(f"YellowPages extraction error: {e}")
        
        if not listings:
            listings = self.extract_raw_fallback(html_content, city, category)
        return listings

# Register directory scrapers
ScraperRegistry.register("SITEMAP", SitemapScraper)
ScraperRegistry.register("YELLOWPAGES", YellowPagesIndiaScraper)
ScraperRegistry.register("GMB", GoogleMapsScraper)
ScraperRegistry.register("LINKEDIN", LinkedInGoogleScraper)
ScraperRegistry.register("DORK", GoogleDorkScraper)
ScraperRegistry.register("GOOGLE_DORK", GoogleDorkScraper)
