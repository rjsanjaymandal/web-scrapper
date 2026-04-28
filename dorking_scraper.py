import asyncio
import logging
import re
import random
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
# from playwright.async_api import Page
Page = dict
from scrapers_registry import BaseScraper
from processing import ProcessingHandler

logger = logging.getLogger(__name__)

class GoogleDorkScraper(BaseScraper):
    """
    Advanced footprint scraper using Search Engine Dorks.
    Fast, reliable, and finds data that directories might miss.
    """
    source_name = "FOOTPRINT"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # Footprint patterns:
        # "category" city "phone" "email"
        query = f'"{category}" {city} "phone" "email"'
        if page > 1:
            start = (page - 1) * 10
            return f"https://www.google.com/search?q={query}&start={start}"
        return f"https://www.google.com/search?q={query}"

    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector("a")
            if link:
                return await link.get_attribute("href")
        except:
            pass
        return None

    async def extract_listings(
        self, page: Page, city: str = None, category: str = None, html_content: str = None
    ) -> List[Dict]:
        listings = []
        try:
            # Human-like delay after arrival
            await asyncio.sleep(random.uniform(1, 3))
            
            # Look for standard Google search result containers
            # Google often changes classes: .g, .tF2Cxc, .MjjYud are common
            cards = await page.query_selector_all("#search .g, .tF2Cxc, .MjjYud")
            
            for card in cards:
                try:
                    title_elem = await card.query_selector("h3")
                    if not title_elem:
                        continue
                        
                    title = await title_elem.inner_text()
                    url = await self.get_detail_url(card)
                    
                    # Extract text from all potential snippet areas in the card
                    # Resilient selectors for snippet text
                    text = await card.inner_text()
                    
                    # Extraction logic (Fast Discovery)
                    # Phone pattern (Indian: 10 digits starting 6-9 or 0 prefix)
                    phone_match = re.search(r'([6-9]\d{9}|0\d{10})', text)
                    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
                    
                    phone = phone_match.group(0) if phone_match else None
                    email = email_match.group(0) if email_match else None
                    
                    if phone or email:
                        listings.append({
                            "name": title.split("-")[0].strip()[:150],
                            "phone": phone,
                            "email": email,
                            "address": text[:300].replace('\n', ' '),
                            "city": city,
                            "source_name": self.source_name,
                            "detail_url": url,
                        })
                except Exception as e:
                    logger.debug(f"Footprint card error: {e}")
                    continue
                    
            logger.info(f"FOOTPRINT: Extracted {len(listings)} potential leads from snippets")
                    
        except Exception as e:
            logger.error(f"Footprint extraction error: {e}")
            
        return listings
