import aiohttp
import asyncio
import logging
import random
import re
import ssl
from typing import List, Dict, Optional, Any
from lxml import etree
from stealth_utils import StealthManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("polite_http_scraper")

class PoliteHTTPScraper:
    """
    Ultra-lightweight, 'polite' HTTP scraper for Level 1 targets (associations, gov boards).
    No Playwright, no Proxies. Strict randomized delays to prevent DDoS/rate limits.
    """
    
    def __init__(self, max_concurrent: int = 2, proxy: str = None):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ua = StealthManager.get_persistent_ua()
        self.proxy = proxy
        self.base_headers = {}  # Set per-request based on is_json_api

    async def __aenter__(self):
        await self._init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _init_session(self, headers: Dict[str, str] = None):
        """Initializes a new aiohttp session with persistent identity."""
        if self.session:
            await self.session.close()
        
        connector = aiohttp.TCPConnector(ssl=False)
        # 2026 Stability Fix: Disable trust_env if proxy is used to prevent 127.0.0.1 loops
        # in environments like Railway/Docker.
        self.session = aiohttp.ClientSession(
            headers=headers or self.base_headers, 
            connector=connector,
            trust_env=False if self.proxy else True
        )

    async def _get_headers(self, is_json_api: bool = False) -> Dict[str, str]:
        """Get headers based on request type."""
        if is_json_api:
            return {
                "User-Agent": self.ua,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        return StealthManager.get_modern_headers(self.ua)

    async def _polite_delay(self):
        """Strict randomized delay between 1.5s and 3.5s to avoid rate limits."""
        delay = random.uniform(1.5, 3.5)
        await asyncio.sleep(delay)

    async def fetch(self, url: str, method: str = "GET", is_json_api: bool = False, headers: Dict = None, **kwargs) -> Optional[aiohttp.ClientResponse]:
        """Fetch URL with strict politeness and aggressive error recovery for SSL/EOF."""
        request_headers = await self._get_headers(is_json_api)
        if headers:
            request_headers.update(headers)
        
        async with self.semaphore:
            # Polite delay BEFORE request (not after)
            await self._polite_delay()
            
            for attempt in range(1, 5):
                try:
                    if not self.session or self.session.closed:
                        await self._init_session(headers)

                    logger.info(f"Fetching: {url} (Attempt {attempt})")
                    response = await self.session.request(
                        method, 
                        url, 
                        timeout=aiohttp.ClientTimeout(total=30), 
                        headers=request_headers, 
                        proxy=self.proxy,
                        **kwargs
                    )
                    
                    if response.status == 200:
                        return response
                        
                    elif response.status in [429, 500, 502, 503, 504]:
                        # Exponential backoff: 30s, 60s, 90s
                        backoff_time = (attempt) * 30
                        logger.warning(f"Server returned {response.status} on {url}. Backing off for {backoff_time}s...")
                        await asyncio.sleep(backoff_time)
                        continue
                    else:
                        logger.error(f"Failed to fetch {url}: Status {response.status}")
                        return response
                        
                except (ssl.SSLEOFError, ConnectionResetError, aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError) as e:
                    logger.warning(f"SSL/EOF/Reset error on {url}: {e}")
                    await self._init_session(headers)
                    backoff = random.uniform(5.0, 15.0) * attempt
                    await asyncio.sleep(backoff)
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    logger.warning(f"Request error on {url} (Attempt {attempt}): {e}")
                    backoff = random.uniform(5.0, 15.0) * attempt
                    await asyncio.sleep(backoff)
                except Exception as e:
                    logger.error(f"Unexpected error fetching {url}: {e}")
                    break
            
            return None

    async def scrape_json_api(self, target_endpoint: str, params: Dict = None, payload: Dict = None, method: str = "GET", pagination_key: str = "page", start_page: int = 1, max_pages: int = 50) -> List[Dict]:
        all_results = []
        current_page = start_page
        if payload and method == "GET": method = "POST"

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
                is_json_api=True,
                params=current_params if current_params else None,
                json=current_payload if current_payload else None
            )
            
            if not response or response.status != 200: break
            try:
                data = await response.json(content_type=None)
            except: break
            
            results = self._parse_json_response(data)
            if not results: break
            all_results.extend(results)
            current_page += 1
            
        return all_results

    def _parse_json_response(self, data: Any) -> List[Dict]:
        if isinstance(data, list): return data
        if isinstance(data, dict):
            for key in ['data', 'results', 'registrants', 'members', 'list', 'items', 'entities']:
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    async def extract_urls_from_sitemap(self, sitemap_url: str, filter_pattern: str = None) -> List[str]:
        response = await self.fetch(sitemap_url)
        if not response or response.status != 200: return []
            
        xml_content = await response.text()
        if not xml_content: return []
            
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
        except Exception as e:
            logger.error(f"Sitemap parsing error: {e}")
        return urls

    @staticmethod
    def extract_viewstate(html_content: str) -> Dict[str, str]:
        tokens = {}
        patterns = {
            '__VIEWSTATE': r'id="__VIEWSTATE"\s+value="([^"]+)"',
            '__EVENTVALIDATION': r'id="__EVENTVALIDATION"\s+value="([^"]+)"',
            '__VIEWSTATEGENERATOR': r'id="__VIEWSTATEGENERATOR"\s+value="([^"]+)"',
            '__EVENTTARGET': r'id="__EVENTTARGET"\s+value="([^"]+)"',
            '__EVENTARGUMENT': r'id="__EVENTARGUMENT"\s+value="([^"]+)"',
        }
        for name, pattern in patterns.items():
            match = re.search(pattern, html_content)
            if match: tokens[name] = match.group(1)
        return tokens
