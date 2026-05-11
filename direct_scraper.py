"""
Direct Scraper - Government & Regulatory Sites
Uses polite HTTP fetching WITHOUT proxies
Optimized for low-blocking sites
"""

import re
import time
import random
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    pass

logger = logging.getLogger(__name__)


class DirectScraperConfig:
    """Configuration for direct scraping without proxies"""
    
    # Respectful timing (between requests)
    MIN_DELAY = 3.0  # 3 seconds minimum
    MAX_DELAY = 7.0  # 7 seconds maximum
    
    # Request timeouts
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 30
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 15  # seconds
    
    # User agents (rotating)
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    
    # Government sites are more forgiving
    GOVERNMENT_DOMAINS = [
        ".gov.in",
        ".nic.in", 
        ".co.in",
        "sebi.gov.in",
        "icai.org",
        "icsi.edu",
        "mca.gov.in",
        "amfiindia.com",
        "nseindia.com",
        "bseindia.com",
        "rbi.org.in",
        "ibbi.gov.in",
        "irdai.gov.in",
    ]


class DirectPoliteFetcher:
    """
    Polite HTTP fetcher without proxies.
    Optimized for government/regulatory sites.
    """
    
    def __init__(self, config: DirectScraperConfig = None):
        self.config = config or DirectScraperConfig()
        self.session = requests.Session()
        self._last_request_time = 0
        
    def _get_random_ua(self) -> str:
        return random.choice(self.config.USER_AGENTS)
    
    def _get_headers(self, referer: str = "https://www.google.com/") -> Dict:
        return {
            "User-Agent": self._get_random_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer": referer,
        }
    
    def _respectful_delay(self):
        """Add delay between requests to be polite"""
        elapsed = time.time() - self._last_request_time
        delay = random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY)
        
        if elapsed < delay:
            time.sleep(delay - elapsed)
        
        self._last_request_time = time.time()
    
    def fetch(self, url: str, referer: str = None) -> Tuple[Optional[str], int]:
        """
        Fetch URL without proxy.
        Returns: (html_content, status_code)
        """
        self._respectful_delay()
        
        headers = self._get_headers(referer or "https://www.google.com/")
        
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=(self.config.CONNECT_TIMEOUT, self.config.READ_TIMEOUT),
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response.text, 200
                
                elif response.status_code in [429, 500, 502, 503]:
                    wait_time = self.config.RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Got {response.status_code} from {url}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    
                elif response.status_code == 403:
                    logger.warning(f"403 Forbidden from {url} - site blocking")
                    return None, 403
                    
                else:
                    return response.text if response.status_code == 200 else None, response.status_code
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout fetching {url}, attempt {attempt + 1}")
                time.sleep(self.config.RETRY_DELAY)
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error for {url}: {e}")
                time.sleep(self.config.RETRY_DELAY)
        
        logger.error(f"Failed to fetch {url} after {self.config.MAX_RETRIES} attempts")
        return None, 0


class SEBIDirectScraper:
    """Direct scraper for SEBI Registered Investment Advisors"""
    
    SOURCE = "SEBI"
    BASE_URL = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = None) -> List[Dict]:
        """Scrape SEBI data directly"""
        results = []
        
        logger.info(f"🔍 Scraping SEBI for city={city}, category={category}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.sebi.gov.in/")
            
            if not html:
                logger.warning(f"SEBI fetch failed with status {status}")
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try multiple table selectors
            table = (
                soup.find('table', {'id': 'sample_1'}) or
                soup.find('table', {'class': 'table-striped'}) or
                soup.find('table', {'border': '1'}) or
                soup.find('table')
            )
            
            if table:
                rows = table.find_all('tr')
                logger.info(f"SEBI: Found {len(rows)} table rows")
                
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 4:
                        try:
                            reg_no = cols[0].get_text(strip=True)
                            name = cols[1].get_text(strip=True)
                            address = cols[2].get_text(strip=True)
                            city_col = cols[3].get_text(strip=True) if len(cols) > 3 else city or ""
                            
                            if name and "Name" not in name and len(name) > 2:
                                results.append({
                                    "name": name[:200],
                                    "phone": None,
                                    "email": None,
                                    "address": address[:300] if address else None,
                                    "city": city_col or city,
                                    "category": category or "Investment Advisors",
                                    "source": self.SOURCE,
                                    "source_url": self.BASE_URL,
                                    "registration_no": reg_no,
                                })
                        except Exception as e:
                            continue
            
            if not results:
                # Fallback: extract from page text
                results = self._extract_from_text(soup.get_text(), city, category)
                
        except Exception as e:
            logger.error(f"SEBI scrape error: {e}")
        
        logger.info(f"SEBI: Extracted {len(results)} records")
        return results
    
    def _extract_from_text(self, text: str, city: str, category: str) -> List[Dict]:
        """Fallback extraction from raw text"""
        results = []
        
        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        phone_pattern = re.compile(r'(\+91[\s.-]?\d{10}|\b\d{10}\b|\b0\d{10,11}\b)')
        
        emails = email_pattern.findall(text)
        phones = phone_pattern.findall(text)
        
        # Create entries from found data
        for email in emails[:20]:
            results.append({
                "name": "SEBI Registered Advisor",
                "email": email,
                "phone": None,
                "city": city,
                "category": category or "Investment Advisors",
                "source": self.SOURCE,
            })
        
        for phone in phones[:20]:
            results.append({
                "name": "SEBI Registered Advisor",
                "phone": phone,
                "email": None,
                "city": city,
                "category": category or "Investment Advisors",
                "source": self.SOURCE,
            })
        
        return results


class ICAIDirectScraper:
    """Direct scraper for ICAI - Chartered Accountants"""
    
    SOURCE = "ICAI"
    BASE_URL = "https://www.icai.org/traceamember.html"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Chartered Accountants") -> List[Dict]:
        """Scrape ICAI member directory directly"""
        results = []
        
        logger.info(f"🔍 Scraping ICAI for city={city}")
        
        # ICAI uses multiple pages based on city
        cities_to_try = [city] if city else ["New Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad", "Pune"]
        
        for c in cities_to_try:
            try:
                # ICAI search URL format
                search_url = f"https://www.icai.org/search?search={c}&type=member"
                
                html, status = self.fetcher.fetch(search_url, "https://www.google.com/")
                
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for member cards/listings
                cards = soup.find_all(['div', 'tr'], class_=lambda x: x and ('member' in x.lower() or 'member' in str(x)))
                
                for card in cards:
                    try:
                        name_elem = card.find(['h3', 'h4', 'strong'])
                        name = name_elem.get_text(strip=True) if name_elem else ""
                        
                        if name and len(name) > 3:
                            # Try to extract email/phone from card
                            text = card.get_text()
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                            
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "city": c,
                                "category": category,
                                "source": self.SOURCE,
                                "source_url": search_url,
                            })
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"ICAI city {c} error: {e}")
                continue
        
        logger.info(f"ICAI: Extracted {len(results)} records")
        return results


class MCADirectScraper:
    """Direct scraper for MCA (Ministry of Corporate Affairs)"""
    
    SOURCE = "MCA"
    BASE_URL = "https://www.mca.gov.in/mca04/ffcs.html"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Company Secretaries") -> List[Dict]:
        """Scrape MCA data directly"""
        results = []
        
        logger.info(f"🔍 Scraping MCA for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.google.com/")
            
            if not html:
                logger.warning(f"MCA fetch failed with status {status}")
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for tables with professional data
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        name = cols[0].get_text(strip=True)
                        details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                        
                        if name and len(name) > 3 and "Name" not in name:
                            results.append({
                                "name": name[:200],
                                "address": details[:300],
                                "city": city,
                                "category": category,
                                "source": self.SOURCE,
                                "source_url": self.BASE_URL,
                            })
                            
        except Exception as e:
            logger.error(f"MCA scrape error: {e}")
        
        logger.info(f"MCA: Extracted {len(results)} records")
        return results


class AMFIDirectScraper:
    """Direct scraper for AMFI - Mutual Fund Agents"""
    
    SOURCE = "AMFI"
    BASE_URL = "https://www.amfiindia.com/locate-distributor"
    API_URL = "https://www.amfiindia.com/api/distributor-agent"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Mutual Fund Agents") -> List[Dict]:
        """Scrape AMFI data directly"""
        results = []
        
        logger.info(f"🔍 Scraping AMFI for city={city}")
        
        try:
            # Try API first
            if city:
                api_url = f"{self.API_URL}?city={city.replace(' ', '%20')}"
                html, status = self.fetcher.fetch(api_url)
            else:
                html, status = self.fetcher.fetch(self.BASE_URL)
            
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for distributor/mutual fund listings
            listings = soup.find_all(['div', 'tr'], class_=lambda x: x and ('distributor' in str(x).lower() or 'mutual' in str(x).lower()))
            
            for listing in listings:
                text = listing.get_text()
                
                # Extract name
                name_match = re.search(r'([A-Z][a-zA-Z\s]+(?:Pvt|Ltd|Inc)?)', text)
                if name_match:
                    name = name_match.group(1)[:200]
                else:
                    continue
                
                # Extract contact info
                email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                
                results.append({
                    "name": name,
                    "email": email_match.group(0) if email_match else None,
                    "phone": phone_match.group(0) if phone_match else None,
                    "city": city,
                    "category": category,
                    "source": self.SOURCE,
                })
                
        except Exception as e:
            logger.error(f"AMFI scrape error: {e}")
        
        logger.info(f"AMFI: Extracted {len(results)} records")
        return results


class NSEDirectScraper:
    """Direct scraper for NSE (National Stock Exchange)"""
    
    SOURCE = "NSE"
    BASE_URL = "https://www.nseindia.com/members/content/member_directory.htm"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Stock Brokers") -> List[Dict]:
        """Scrape NSE member directory directly"""
        results = []
        
        logger.info(f"🔍 Scraping NSE for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.nseindia.com/")
            
            if not html:
                logger.warning(f"NSE fetch failed with status {status}")
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            
            table = soup.find('table', {'id': 'memberDirectoryTable'}) or soup.find('table')
            
            if table:
                rows = table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        try:
                            broker_code = cols[0].get_text(strip=True)
                            name = cols[1].get_text(strip=True)
                            address = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                            
                            if name and "Name" not in name and len(name) > 3:
                                results.append({
                                    "name": name[:200],
                                    "address": address[:300],
                                    "city": city or "Multiple",
                                    "category": category,
                                    "source": self.SOURCE,
                                    "source_url": self.BASE_URL,
                                    "registration_no": broker_code,
                                })
                        except:
                            continue
                            
        except Exception as e:
            logger.error(f"NSE scrape error: {e}")
        
        logger.info(f"NSE: Extracted {len(results)} records")
        return results


class GeneralDirectScraper:
    """General purpose direct scraper for various sources"""
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape_url(self, url: str, referer: str = "https://www.google.com/") -> List[Dict]:
        """Scrape any URL and extract contact information"""
        results = []
        
        logger.info(f"🔍 Direct scraping: {url}")
        
        try:
            html, status = self.fetcher.fetch(url, referer)
            
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract emails
            emails = set(re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', soup.get_text()))
            
            # Extract phone numbers
            phones = set(re.findall(r'(\+91[\s.-]?\d{10}|\b\d{10}\b|\b0\d{10,11}\b)', soup.get_text()))
            
            for email in emails:
                results.append({
                    "name": "Direct Contact",
                    "email": email,
                    "phone": None,
                    "source": "DIRECT",
                    "source_url": url,
                })
            
            for phone in phones:
                results.append({
                    "name": "Direct Contact",
                    "phone": phone,
                    "email": None,
                    "source": "DIRECT",
                    "source_url": url,
                })
                
        except Exception as e:
            logger.error(f"Direct scrape error for {url}: {e}")
        
        return results


# Registry for easy access
SCRAPERS = {
    "SEBI": SEBIDirectScraper,
    "ICAI": ICAIDirectScraper,
    "MCA": MCADirectScraper,
    "AMFI": AMFIDirectScraper,
    "NSE": NSEDirectScraper,
}

def get_scraper(source: str) -> Optional[object]:
    """Get scraper class by source name"""
    return SCRAPERS.get(source.upper())


if __name__ == "__main__":
    # Test direct scraping
    logging.basicConfig(level=logging.INFO)
    
    fetcher = DirectPoliteFetcher()
    
    print("\n=== Testing SEBI Direct Scraper ===")
    sebi_scraper = SEBIDirectScraper(fetcher)
    results = sebi_scraper.scrape(city="Delhi", category="Investment Advisors")
    print(f"SEBI Results: {len(results)} records")
    
    print("\n=== Testing NSE Direct Scraper ===")
    nse_scraper = NSEDirectScraper(fetcher)
    results = nse_scraper.scrape(city="Mumbai", category="Stock Brokers")
    print(f"NSE Results: {len(results)} records")