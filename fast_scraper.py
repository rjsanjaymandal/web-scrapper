import aiohttp
import asyncio
import logging
import random
import re
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
import json
import os

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

    async def scrape_json_api(self, target_endpoint: str, params: Dict = None, payload: Dict = None, method: str = "GET", pagination_key: str = "page", start_page: int = 1, max_pages: int = 10) -> List[Dict]:
        """
        Generic function to loop through paginated JSON endpoints.
        """
        all_results = []
        current_page = start_page
        
        # Determine internal method if not explicitly set (handle params vs payload)
        if payload and method == "GET":
            method = "POST" # Usually if we have a payload we want POST

        while current_page <= max_pages:
            logger.info(f"Scraping API page {current_page}...")
            
            # Prepare request parameters
            current_params = params.copy() if params else {}
            current_payload = payload.copy() if payload else {}
            
            # Add pagination to the correct bucket
            if method == "GET" or not current_payload:
                current_params[pagination_key] = current_page
            else:
                current_payload[pagination_key] = current_page
            
            try:
                async with self.session.request(
                    method, 
                    target_endpoint, 
                    params=current_params if current_params else None,
                    json=current_payload if current_payload else None,
                    timeout=30
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"API returned status {resp.status} on page {current_page}")
                        break
                        
                    data = await resp.json()
                
                # Extract results (Assuming standard list in response)
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
            # Look for common result keys (Added registrants for SEBI)
            for key in ['data', 'results', 'registrants', 'members', 'list', 'items', 'entities']:
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

async def fast_scrape_all(config_dict: Dict, cities: List[str], categories: List[str]) -> int:
    """
    STANDALONE entry point for automatic, high-speed extraction.
    Used by automate_100_cities.py to run mass-scraping logic.
    """
    from api_handlers import OfficialAPIHandlers
    from redis_manager import RedisQueueManager
    
    total_leads = 0
    redis_url = os.environ.get("REDIS_URL") or config_dict.get("scraper_settings", {}).get("redis_url")
    
    if not redis_url:
        logger.error("REDIS_URL not found. Cannot push results.")
        return 0

    redis_manager = RedisQueueManager(redis_url)
    try:
        await redis_manager.connect()
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return 0

    async with FastHTTPScraper(max_concurrent=config_dict.get("scraper_settings", {}).get("max_concurrent", 5)) as engine:
        for city in cities:
            for category in categories:
                logger.info(f"⚡ High-Speed Cycle: {city} | {category}")
                
                # Official Registries are usually category-agnostic or broad
                # We'll map category names to registry sources if possible
                target_sources = ["SEBI", "IBBI"] # Default low-hanging fruit
                
                for source in target_sources:
                    try:
                        leads = await OfficialAPIHandlers.dispatch(source, engine, city)
                        if leads:
                            # Add metadata before pushing to Redis
                            for lead in leads:
                                lead["category"] = category
                                lead["city"] = city
                                lead["scraped_at"] = datetime.now().isoformat()
                            
                            await redis_manager.push_results(leads)
                            total_leads += len(leads)
                            logger.info(f"✅ Discovered {len(leads)} leads from {source} in {city}")
                    except Exception as e:
                        logger.error(f"Error extracting from {source}: {e}")
                
                # Polite delay between city/category pairs
                await asyncio.sleep(random.uniform(1, 3))
                
    await redis_manager.disconnect()
    return total_leads

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
