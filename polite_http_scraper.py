import aiohttp
import asyncio
import logging
import random
import re
from typing import List, Dict, Optional, Any
from lxml import etree

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("polite_http_scraper")

class PoliteHTTPScraper:
    """
    Ultra-lightweight, 'polite' HTTP scraper for Level 1 targets (associations, gov boards).
    No Playwright, no Proxies. Strict randomized delays to prevent DDoS/rate limits.
    """
    
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, application/xhtml+xml, application/xml;q=0.9, image/avif, image/webp, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0"
    }

    def __init__(self, max_concurrent: int = 5):
        # Even with max_concurrent, we enforce strict domain politeness
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.DEFAULT_HEADERS)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _polite_delay(self):
        """Strict randomized delay between 1.5s and 3.5s to avoid rate limits."""
        delay = random.uniform(1.5, 3.5)
        logger.debug(f"Polite delay: sleeping for {delay:.2f}s")
        await asyncio.sleep(delay)

    async def fetch(self, url: str, method: str = "GET", **kwargs) -> Optional[aiohttp.ClientResponse]:
        """Fetch URL with strict politeness, backoff for 429/50x, and NO proxies."""
        async with self.semaphore:
            for attempt in range(1, 4):
                try:
                    logger.info(f"Fetching: {url} (Attempt {attempt})")
                    response = await self.session.request(method, url, timeout=30, **kwargs)
                    
                    if response.status == 200:
                        await self._polite_delay()
                        return response
                        
                    elif response.status == 429 or 500 <= response.status <= 504:
                        # Too Many Requests or Server Error -> Backoff 30 to 60 seconds
                        backoff = random.uniform(30.0, 60.0)
                        logger.warning(f"Server returned {response.status} on {url}. Backing off for {backoff:.2f} seconds...")
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        logger.error(f"Failed to fetch {url}: Status {response.status}")
                        await self._polite_delay()
                        return response # Return it anyway, caller can handle 404/403
                        
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    backoff = random.uniform(30.0, 60.0)
                    logger.warning(f"Exception during fetch. Backing off for {backoff:.2f} seconds...")
                    await asyncio.sleep(backoff)
            
            return None

    async def scrape_json_api(self, target_endpoint: str, params: Dict = None, payload: Dict = None, method: str = "GET", pagination_key: str = "page", start_page: int = 1, max_pages: int = 50) -> List[Dict]:
        """
        Generic function to loop through paginated JSON endpoints.
        Automatically increments pagination until no results are returned.
        """
        all_results = []
        current_page = start_page
        
        if payload and method == "GET":
            method = "POST"

        while current_page <= max_pages:
            logger.info(f"Scraping JSON API page {current_page}...")
            
            current_params = params.copy() if params else {}
            current_payload = payload.copy() if payload else {}
            
            if method == "GET" or not current_payload:
                current_params[pagination_key] = current_page
            else:
                current_payload[pagination_key] = current_page
            
            response = await self.fetch(
                target_endpoint, 
                method=method, 
                params=current_params if current_params else None,
                json=current_payload if current_payload else None
            )
            
            if not response or response.status != 200:
                logger.error(f"API extraction stopped at page {current_page} due to bad response.")
                break
                
            try:
                data = await response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON on page {current_page}: {e}")
                break
            
            results = self._parse_json_response(data)
            if not results:
                logger.info(f"No more results found in JSON on page {current_page}. Stopping pagination.")
                break
                
            all_results.extend(results)
            logger.info(f"Extracted {len(results)} items from page {current_page} (Total: {len(all_results)})")
            
            current_page += 1
            
        return all_results

    def _parse_json_response(self, data: Any) -> List[Dict]:
        """Attempts to dynamically locate the list of items inside a JSON payload."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ['data', 'results', 'registrants', 'members', 'list', 'items', 'entities']:
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    async def extract_urls_from_sitemap(self, sitemap_url: str, filter_pattern: str = None) -> List[str]:
        """
        Downloads a sitemap.xml, parses it, and returns <loc> URLs matching the regex filter.
        """
        response = await self.fetch(sitemap_url)
        if not response or response.status != 200:
            logger.error(f"Failed to fetch sitemap: {sitemap_url}")
            return []
            
        xml_content = await response.text()
        if not xml_content:
            return []
            
        urls = []
        try:
            root = etree.fromstring(xml_content.encode('utf-8'))
            ns = {'ns': root.nsmap.get(None, 'http://www.sitemaps.org/schemas/sitemap/0.9')}
            
            locs = root.xpath('//ns:loc', namespaces=ns)
            for loc in locs:
                url = loc.text
                if filter_pattern and not re.search(filter_pattern, url, re.IGNORECASE):
                    continue
                urls.append(url)
                
            logger.info(f"Extracted {len(urls)} URLs matching '{filter_pattern}' from sitemap: {sitemap_url}")
        except Exception as e:
            logger.error(f"Sitemap parsing error: {e}")
            
        return urls
