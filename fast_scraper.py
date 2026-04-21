import aiohttp
import asyncio
import logging
import random
import re
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from lxml import etree
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fast_http_scraper")

class FastHTTPScraper:
    """
    Ultra-fast, lightweight HTTP scraper using aiohttp.
    Bypasses Playwright/Puppeteer for low-security targets and direct APIs.
    """
    
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, application/xhtml+xml, application/xml;q=0.9, image/avif, image/webp, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    def __init__(self, max_concurrent: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.DEFAULT_HEADERS)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch(self, url: str, method: str = "GET", **kwargs) -> Optional[str]:
        """Fetch URL with concurrency limit and retries."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    logger.info(f"Fetching: {url} (Attempt {attempt+1})")
                    async with self.session.request(method, url, timeout=30, **kwargs) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:
                            wait = 2 ** attempt + random.random()
                            logger.warning(f"Rate limited on {url}. Waiting {wait:.2f}s")
                            await asyncio.sleep(wait)
                        else:
                            logger.error(f"Failed to fetch {url}: Status {response.status}")
                            break
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    await asyncio.sleep(1)
        return None

    async def scrape_json_api(self, target_endpoint: str, payload: Dict = None, method: str = "POST", pagination_key: str = "page", start_page: int = 1, max_pages: int = 10) -> List[Dict]:
        """
        Generic function to loop through paginated JSON endpoints.
        """
        all_results = []
        current_page = start_page
        
        while current_page <= max_pages:
            logger.info(f"Scraping API page {current_page}...")
            
            # Prepare request parameters
            params = payload.copy() if payload else {}
            params[pagination_key] = current_page
            
            try:
                if method == "POST":
                    async with self.session.post(target_endpoint, json=params) as resp:
                        data = await resp.json()
                else:
                    async with self.session.get(target_endpoint, params=params) as resp:
                        data = await resp.json()
                
                # Extract results (Assuming standard list in response)
                # This part usually needs source-specific override
                results = self._parse_json_response(data)
                if not results:
                    logger.info("No more results in JSON API.")
                    break
                    
                all_results.extend(results)
                logger.info(f"Extracted {len(results)} items from page {current_page}")
                
                current_page += 1
                await asyncio.sleep(random.uniform(1, 3)) # Polite jitter
                
            except Exception as e:
                logger.error(f"API scraping error on page {current_page}: {e}")
                break
                
        return all_results

    def _parse_json_response(self, data: Any) -> List[Dict]:
        """Override this to handle specific JSON structures."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Look for common result keys
            for key in ['data', 'results', 'members', 'list', 'items']:
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    async def extract_urls_from_sitemap(self, sitemap_url: str, filter_pattern: str = None) -> List[str]:
        """
        Downloads a sitemap.xml, parses it, and returns profile URLs.
        """
        xml_content = await self.fetch(sitemap_url)
        if not xml_content:
            return []
            
        urls = []
        try:
            root = etree.fromstring(xml_content.encode('utf-8'))
            # Support both standard sitemap and sitemap index namespaces
            ns = {'ns': root.nsmap.get(None, 'http://www.sitemaps.org/schemas/sitemap/0.9')}
            
            locs = root.xpath('//ns:loc', namespaces=ns)
            for loc in locs:
                url = loc.text
                if filter_pattern and not re.search(filter_pattern, url):
                    continue
                urls.append(url)
                
            logger.info(f"Extracted {len(urls)} URLs from sitemap: {sitemap_url}")
        except Exception as e:
            logger.error(f"Sitemap parsing error: {e}")
            
        return urls

    async def scrape_profiles_parallel(self, urls: List[str], parse_callback) -> List[Dict]:
        """Scrape multiple profile URLs in parallel (high speed)."""
        tasks = []
        for url in urls:
            tasks.append(self._scrape_single_profile(url, parse_callback))
        
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]

    async def _scrape_single_profile(self, url: str, parse_callback) -> Optional[Dict]:
        html = await self.fetch(url)
        if not html:
            return None
        return parse_callback(html, url)

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    async def test():
        async with FastHTTPScraper(max_concurrent=5) as scraper:
            # Test Sitemap
            # urls = await scraper.extract_urls_from_sitemap("https://example.com/sitemap.xml", filter_pattern="/profile/")
            # print(f"Found {len(urls)} target URLs")
            
            # Test JSON API Template
            # results = await scraper.scrape_json_api("https://api.example.com/v1/search", {"q": "financial"})
            # print(f"Scraped {len(results)} items")
            pass
            
    asyncio.run(test())
