import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper, ScraperRegistry

logger = logging.getLogger(__name__)

class JustDialScraper(BaseScraper):
    source_name = "JUSTDIAL"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat_slug = category.lower().replace(" ", "-")
        return f"https://www.justdial.com/{city}/{cat_slug}/page-{page}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            if html_content:
                content = html_content
            elif page and hasattr(page, 'content'):
                content = await page.content()
            else:
                content = ""
            
            if not content: return []
            soup = BeautifulSoup(content, 'lxml')
            cards = soup.select('.cntanr, .store-details, .result-card')
            for card in cards:
                name = card.select_one('.lng_cont_name, .store-name, h2')
                phone = card.select_one('.contact-info, .mobiles, .call-action')
                addr = card.select_one('.cont_fl_addr, .address, .city-name')
                if name:
                    listings.append({
                        'name': name.get_text(strip=True),
                        'phone': self._clean_phone(phone.get_text(strip=True)) if phone else None,
                        'address': addr.get_text(strip=True) if addr else None,
                        'city': city,
                        'source': 'JUSTDIAL'
                    })
            if not listings:
                listings = self.extract_raw_fallback(content, city, category)
        except Exception as e:
            logger.error(f"JustDial Scraper Error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        digits = re.sub(r'[^\d]', '', phone)
        return digits[-10:] if len(digits) >= 10 else digits

class IndiaMartScraper(BaseScraper):
    source_name = "INDIAMART"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat_slug = category.lower().replace(" ", "+")
        city_slug = city.lower().replace(" ", "+")
        return f"https://www.indiamart.com/search.html?ss={cat_slug}&cq={city_slug}&m=1&pn={page}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        # High-speed extraction often works best via raw regex on IndiaMart due to heavy obfuscation
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class SulekhaScraper(BaseScraper):
    source_name = "SULEKHA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = category.lower().replace(' ', '-')
        return f"https://www.sulekha.com/local/{cat}/{city.lower()}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class ClickIndiaScraper(BaseScraper):
    source_name = "CLICKINDIA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = category.lower().replace(' ', '-')
        return f"https://www.clickindia.com/{cat}/{city.lower()}/?page={page}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class GrotalScraper(BaseScraper):
    source_name = "GROTAL"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = category.replace(' ', '-')
        return f"https://www.grotal.com/{city.title()}/{cat}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class YellowPagesScraper(BaseScraper):
    source_name = "YELLOWPAGES"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.yellowpages.in/{city}/{category.replace(' ', '-')}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class TradeIndiaScraper(BaseScraper):
    source_name = "TRADEINDIA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.tradeindia.com/search.html?keyword={category.replace(' ', '+')}&city={city}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        if html_content:
            content = html_content
        elif page and hasattr(page, 'content'):
            content = await page.content()
        else:
            content = ""
        return self.extract_raw_fallback(content, city, category)

class ExportersIndiaScraper(BaseScraper):
    source_name = "EXPORTERSINDIA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        q = f"{category} in {city}".replace(" ", "+")
        return f"https://www.exportersindia.com/search.php?term={q}&page={page}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        content = html_content or await (page.content() if hasattr(page, 'content') else "")
        return self.extract_raw_fallback(content, city, category)

# Register business scrapers
ScraperRegistry.register("JUSTDIAL", JustDialScraper)
ScraperRegistry.register("INDIAMART", IndiaMartScraper)
ScraperRegistry.register("SULEKHA", SulekhaScraper)
ScraperRegistry.register("CLICKINDIA", ClickIndiaScraper)
ScraperRegistry.register("GROTAL", GrotalScraper)
ScraperRegistry.register("YELLOWPAGES", YellowPagesScraper)
ScraperRegistry.register("TRADEINDIA", TradeIndiaScraper)
ScraperRegistry.register("EXPORTERSINDIA", ExportersIndiaScraper)

class AskLailaScraper(BaseScraper):
    source_name = "ASKLAILA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.asklaila.com/search/{city}/{category}/{page}/"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        content = html_content or await (page.content() if hasattr(page, 'content') else "")
        return self.extract_raw_fallback(content, city, category)

class VykariScraper(BaseScraper):
    source_name = "VYKARI"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.vykari.com/search?q={category}&l={city}&p={page}"
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        content = html_content or await (page.content() if hasattr(page, 'content') else "")
        return self.extract_raw_fallback(content, city, category)

ScraperRegistry.register("ASKLAILA", AskLailaScraper)
ScraperRegistry.register("VYKARI", VykariScraper)
