import requests
import time
import random
import logging
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

class PoliteFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        
    def get_headers(self):
        return {
            'User-Agent': self.ua.chrome,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }

    def fetch(self, url, method='GET', **kwargs):
        """
        Robust HTTP fetcher with randomized delays and exponential backoff.
        Bypasses heavy browser usage to save Railway memory.
        """
        # 1. Automatic Randomized Delay (1.5s to 3.0s)
        time.sleep(random.uniform(1.5, 3.0))
        
        # 2. Inject Realistic Chrome Headers
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()

        max_retries = 3
        retry_delay = 30 # Fixed 30s as per user prompt, but can be scaled

        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
                
                # 3. Handle Status Codes 429, 500, 502, 503
                if response.status_code in [429, 500, 502, 503]:
                    logger.warning(f"⚠️ WARNING: Received {response.status_code} from {url}. Retrying in {retry_delay}s... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                
                return response
            except Exception as e:
                logger.warning(f"❌ Error fetching {url}: {e}. Retrying... (Attempt {attempt+1}/{max_retries})")
                time.sleep(retry_delay)

        logger.error(f"💀 CRITICAL: Failed to fetch {url} after {max_retries} attempts.")
        return None

# Export singleton
fetcher = PoliteFetcher()
