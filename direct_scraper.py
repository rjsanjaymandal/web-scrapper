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
from urllib.parse import urlencode

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    pass

logger = logging.getLogger(__name__)


class DirectScraperConfig:
    """Configuration for direct scraping without proxies"""
    
    MIN_DELAY = 4.0
    MAX_DELAY = 10.0
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 15
    
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    CA_PRIORITY_CITIES = [
        "Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad",
        "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow",
    ]

    CA_CONNECT_SERVICES = ["Audit", "Direct Taxes", "Goods and Services Tax"]

    CITY_STATE_MAP = {
        "delhi": "Delhi", "new delhi": "Delhi",
        "mumbai": "Maharashtra", "pune": "Maharashtra", "nagpur": "Maharashtra", "nashik": "Maharashtra", "thane": "Maharashtra", "aurangabad": "Maharashtra",
        "bangalore": "Karnataka", "bengaluru": "Karnataka", "mysore": "Karnataka", "hubli": "Karnataka",
        "chennai": "Tamil Nadu", "coimbatore": "Tamil Nadu", "madurai": "Tamil Nadu", "salem": "Tamil Nadu", "tiruchirappalli": "Tamil Nadu",
        "hyderabad": "Telangana", "warangal": "Telangana",
        "kolkata": "West Bengal", "asansol": "West Bengal",
        "ahmedabad": "Gujarat", "surat": "Gujarat", "vadodara": "Gujarat", "rajkot": "Gujarat", "bhavnagar": "Gujarat",
        "jaipur": "Rajasthan", "jodhpur": "Rajasthan", "udaipur": "Rajasthan", "kota": "Rajasthan",
        "lucknow": "Uttar Pradesh", "kanpur": "Uttar Pradesh", "noida": "Uttar Pradesh", "ghaziabad": "Uttar Pradesh", "agra": "Uttar Pradesh", "varanasi": "Uttar Pradesh", "meerut": "Uttar Pradesh",
        "patna": "Bihar", "gaya": "Bihar",
        "indore": "Madhya Pradesh", "bhopal": "Madhya Pradesh", "gwalior": "Madhya Pradesh", "jabalpur": "Madhya Pradesh",
        "kochi": "Kerala", "thiruvananthapuram": "Kerala", "kozhikode": "Kerala",
        "chandigarh": "Chandigarh", "ludhiana": "Punjab", "amritsar": "Punjab", "jalandhar": "Punjab",
        "raipur": "Chhattisgarh", "bhilai": "Chhattisgarh",
        "ranchi": "Jharkhand", "jamshedpur": "Jharkhand",
        "bhubaneswar": "Odisha", "cuttack": "Odisha",
        "guwahati": "Assam",
    }
    
    GOVERNMENT_DOMAINS = [
        ".gov.in", ".nic.in", ".co.in",
        "sebi.gov.in", "icai.org", "icsi.edu", "mca.gov.in",
        "amfiindia.com", "nseindia.com", "bseindia.com",
        "rbi.org.in", "ibbi.gov.in", "irdai.gov.in",
    ]


class DirectPoliteFetcher:
    def __init__(self, config: DirectScraperConfig = None):
        self.config = config or DirectScraperConfig()
        self.session = requests.Session()
        self._last_request_time = 0
        self._session_ua = random.choice(self.config.USER_AGENTS)
    
    def _get_random_ua(self) -> str:
        return self._session_ua
    
    def _get_headers(self, referer: str = "https://www.google.com/") -> Dict:
        ua = self._get_random_ua()
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Referer": referer,
            "Cache-Control": "max-age=0",
        }
    
    def _respectful_delay(self):
        elapsed = time.time() - self._last_request_time
        delay = random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()
    
    def fetch(self, url: str, referer: str = None) -> Tuple[Optional[str], int]:
        self._respectful_delay()
        headers = self._get_headers(referer or "https://www.google.com/")
        
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.session.get(
                    url, headers=headers,
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
                    logger.warning(f"403 Forbidden from {url}")
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
    SOURCE = "SEBI"
    BASE_URL = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = None) -> List[Dict]:
        results = []
        logger.info(f"Scraping SEBI for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.sebi.gov.in/")
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
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
                                    "name": name[:200], "phone": None, "email": None,
                                    "address": address[:300] if address else None,
                                    "city": city_col or city,
                                    "category": category or "Investment Advisors",
                                    "source": self.SOURCE, "source_url": self.BASE_URL,
                                    "registration_no": reg_no, "license_no": reg_no,
                                })
                        except:
                            continue
            
            if not results:
                results = self._extract_from_text(soup.get_text(), city, category)
        except Exception as e:
            logger.error(f"SEBI scrape error: {e}")
        
        logger.info(f"SEBI: Extracted {len(results)} records")
        return results
    
    def _extract_from_text(self, text: str, city: str, category: str) -> List[Dict]:
        results = []
        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        phone_pattern = re.compile(r'(\+91[\s.-]?\d{10}|\b\d{10}\b|\b0\d{10,11}\b)')
        
        for email in email_pattern.findall(text)[:20]:
            results.append({"name": "SEBI Registered Advisor", "email": email, "phone": None,
                           "city": city, "category": category or "Investment Advisors", "source": self.SOURCE})
        for phone in phone_pattern.findall(text)[:20]:
            results.append({"name": "SEBI Registered Advisor", "phone": phone, "email": None,
                           "city": city, "category": category or "Investment Advisors", "source": self.SOURCE})
        return results


class ICAIDirectScraper:
    SOURCE = "ICAI"
    BASE_URL = "https://www.icai.org/traceamember.html"
    CA_CONNECT_SEARCH_URL = "https://caconnect.icai.org/search"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Chartered Accountants") -> List[Dict]:
        caconnect_results = self._scrape_caconnect(city, category)
        if caconnect_results:
            logger.info(f"ICAI CA Connect: Extracted {len(caconnect_results)} records")
            return caconnect_results

        results = []
        logger.info(f"Scraping ICAI for city={city}")
        cities_to_try = [city] if city else self.fetcher.config.CA_PRIORITY_CITIES
        
        for c in cities_to_try:
            try:
                search_url = f"https://www.icai.org/search?search={c}&type=member"
                html, status = self.fetcher.fetch(search_url, "https://www.google.com/")
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                cards = soup.find_all(['div', 'tr'], class_=lambda x: x and 'member' in str(x).lower())
                
                for card in cards:
                    try:
                        name_elem = card.find(['h3', 'h4', 'strong'])
                        name = name_elem.get_text(strip=True) if name_elem else ""
                        if name and len(name) > 3:
                            text = card.get_text()
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "city": c, "category": category, "source": self.SOURCE,
                                "source_url": search_url, "membership_no": None,
                            })
                    except:
                        continue
            except Exception as e:
                logger.warning(f"ICAI city {c} error: {e}")
                continue
        
        logger.info(f"ICAI: Extracted {len(results)} records")
        return results

    def _state_for_city(self, city: str) -> str:
        if not city:
            return ""
        return self.fetcher.config.CITY_STATE_MAP.get(city.strip().lower(), "")

    def _services_for_category(self, category: str) -> List[str]:
        category_text = (category or "").lower()
        if "gst" in category_text:
            return ["Goods and Services Tax"]
        if "tax" in category_text:
            return ["Direct Taxes", "Goods and Services Tax"]
        if "audit" in category_text:
            return ["Audit"]
        return list(self.fetcher.config.CA_CONNECT_SERVICES)

    def _scrape_caconnect(self, city: str = None, category: str = None) -> List[Dict]:
        cities_to_try = [city] if city else self.fetcher.config.CA_PRIORITY_CITIES
        services_to_try = self._services_for_category(category)
        results = []
        seen = set()

        for target_city in cities_to_try:
            state = self._state_for_city(target_city)
            if not state:
                continue

            for service in services_to_try:
                query = urlencode({"services": service, "state": state, "city": target_city})
                search_url = f"{self.CA_CONNECT_SEARCH_URL}?{query}"
                html, status = self.fetcher.fetch(search_url, "https://caconnect.icai.org/search-your-ca")
                if not html or status != 200:
                    continue

                soup = BeautifulSoup(html, "html.parser")
                cards = soup.select(".searchBox.scr")
                logger.info(f"ICAI CA Connect: {target_city}/{service} returned {len(cards)} cards")

                for card in cards:
                    name_el = card.select_one("p b")
                    name = name_el.get_text(" ", strip=True) if name_el else ""
                    name = re.sub(r"\s+", " ", name).strip()
                    if not name:
                        continue

                    address_el = card.select_one(".state")
                    address = address_el.get_text(" ", strip=True) if address_el else ""
                    address = re.sub(r"\s+", " ", address).strip()

                    city_el = card.select_one(".pcity")
                    listed_city = ""
                    if city_el:
                        listed_city = city_el.get_text(" ", strip=True)
                        listed_city = re.sub(r"^Professional City:\s*", "", listed_city, flags=re.I).strip()

                    href_el = card.select_one("a[href*='Profile']")
                    source_url = href_el.get("href") if href_el else search_url
                    profile_id = None
                    if source_url:
                        profile_match = re.search(r"/(?:member|firm)Profile/(\d+)/", source_url)
                        if profile_match:
                            profile_id = profile_match.group(1)

                    services = [btn.get_text(" ", strip=True) for btn in card.select(".services_area .boxCe") if btn.get_text(" ", strip=True)]

                    key = profile_id or f"{name}|{address}|{listed_city or target_city}"
                    if key in seen:
                        continue
                    seen.add(key)

                    results.append({
                        "name": name[:200], "phone": None, "email": None,
                        "address": address[:300] if address else None,
                        "city": listed_city or target_city, "state": state,
                        "category": "Chartered Accountants", "source": self.SOURCE,
                        "source_url": source_url,
                        "membership_no": f"CAConnect-{profile_id}" if profile_id else None,
                        "area": ", ".join(services[:4]) if services else service,
                    })

        return results


class MCADirectScraper:
    SOURCE = "MCA"
    BASE_URL = "https://www.mca.gov.in/mca04/ffcs.html"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Company Secretaries") -> List[Dict]:
        results = []
        logger.info(f"Scraping MCA for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.google.com/")
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        name = cols[0].get_text(strip=True)
                        details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                        if name and len(name) > 3 and "Name" not in name:
                            results.append({
                                "name": name[:200], "address": details[:300],
                                "city": city, "category": category, "source": self.SOURCE, "source_url": self.BASE_URL,
                            })
        except Exception as e:
            logger.error(f"MCA scrape error: {e}")
        
        logger.info(f"MCA: Extracted {len(results)} records")
        return results


class AMFIDirectScraper:
    SOURCE = "AMFI"
    BASE_URL = "https://www.amfiindia.com/locate-distributor"
    API_URL = "https://www.amfiindia.com/api/distributor-agent"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Mutual Fund Agents") -> List[Dict]:
        results = []
        logger.info(f"Scraping AMFI for city={city}")
        
        try:
            if city:
                api_url = f"{self.API_URL}?city={city.replace(' ', '%20')}"
                html, status = self.fetcher.fetch(api_url)
            else:
                html, status = self.fetcher.fetch(self.BASE_URL)
            
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            listings = []
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        name = cols[0].get_text(strip=True)
                        contact = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                        if len(name) > 3:
                            listings.append((name, contact))
            
            if not listings:
                div_listings = soup.find_all(['div', 'tr'], class_=lambda x: x and ('distributor' in str(x).lower() or 'mutual' in str(x).lower()))
                for l in div_listings:
                    listings.append((None, l.get_text()))

            for name_text, full_text in listings:
                text = full_text or ""
                if not name_text:
                    name_match = re.search(r'([A-Z][a-zA-Z\s]+(?:Pvt|Ltd|Inc)?)', text)
                    name = name_match.group(1)[:200] if name_match else None
                else:
                    name = name_text[:200]

                if not name or "Name" in name:
                    continue
                
                email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                
                results.append({
                    "name": name, "email": email_match.group(0) if email_match else None,
                    "phone": phone_match.group(0) if phone_match else None,
                    "city": city, "category": category, "source": self.SOURCE, "source_url": self.BASE_URL
                })
        except Exception as e:
            logger.error(f"AMFI scrape error: {e}")
        
        logger.info(f"AMFI: Extracted {len(results)} records")
        return results


class NSEDirectScraper:
    SOURCE = "NSE"
    BASE_URL = "https://enit.nseindia.com/MemDirWeb/searchBrokers_Beta?step=searchBrokersList"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Stock Brokers") -> List[Dict]:
        results = []
        logger.info(f"Scraping NSE for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.nseindia.com/")
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', {'id': 'memberDirectoryTable'}) or soup.find('table')
            
            if table:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        try:
                            broker_code = cols[0].get_text(strip=True)
                            name = cols[1].get_text(strip=True)
                            address = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                            if name and "Name" not in name and len(name) > 3:
                                results.append({
                                    "name": name[:200], "address": address[:300],
                                    "city": city or "Multiple", "category": category,
                                    "source": self.SOURCE, "source_url": self.BASE_URL,
                                    "registration_no": broker_code, "license_no": broker_code,
                                })
                        except:
                            continue
        except Exception as e:
            logger.error(f"NSE scrape error: {e}")
        
        logger.info(f"NSE: Extracted {len(results)} records")
        return results


class BCIDirectScraper:
    SOURCE = "BAR_COUNCIL"
    BASE_URL = "https://www.barcouncilofindia.org/advocates/"
    SEARCH_URL = "https://www.barcouncilofindia.org/advocates/search-advocate/"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Lawyers") -> List[Dict]:
        results = []
        logger.info(f"Scraping Bar Council for city={city}")
        
        cities = [city] if city else ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad",
                                       "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
        
        for c in cities:
            try:
                search_url = f"https://www.barcouncilofindia.org/advocates/search-advocate/?search={c}"
                html, status = self.fetcher.fetch(search_url, "https://www.google.com/")
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                cards = soup.find_all(['div', 'li', 'article'], class_=lambda x: x and 'advocate' in str(x).lower())
                
                if not cards:
                    tables = soup.find_all('table')
                    for table in tables:
                        for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                name = cols[0].get_text(strip=True)
                                details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                                if name and len(name) > 3 and "Name" not in name:
                                    email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', details)
                                    phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', details)
                                    results.append({
                                        "name": name[:200],
                                        "email": email_match.group(0) if email_match else None,
                                        "phone": phone_match.group(0) if phone_match else None,
                                        "address": details[:300], "city": c, "category": category,
                                        "source": self.SOURCE, "source_url": search_url,
                                    })
                else:
                    for card in cards:
                        name_elem = card.find(['h3', 'h4', 'strong'])
                        name = name_elem.get_text(strip=True) if name_elem else ""
                        if name and len(name) > 3:
                            text = card.get_text()
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "city": c, "category": category, "source": self.SOURCE, "source_url": search_url,
                            })
            except Exception as e:
                logger.warning(f"Bar Council city {c} error: {e}")
                continue
        
        logger.info(f"Bar Council: Extracted {len(results)} records")
        return results


class RBIDirectScraper:
    SOURCE = "RBI"
    BASE_URL = "https://www.rbi.org.in/Scripts/BS_NBFCList.aspx"
    SEARCH_URL = "https://www.rbi.org.in/Scripts/WahherView.aspx?Id=1"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "NBFC") -> List[Dict]:
        results = []
        logger.info(f"Scraping RBI for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.google.com/")
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        try:
                            company_name = cols[0].get_text(strip=True)
                            category_type = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            contact = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            if company_name and len(company_name) > 3 and "Company" not in company_name:
                                email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', contact)
                                phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', contact)
                                results.append({
                                    "name": company_name[:200],
                                    "email": email_match.group(0) if email_match else None,
                                    "phone": phone_match.group(0) if phone_match else None,
                                    "address": contact[:300], "city": city,
                                    "category": category_type or category,
                                    "source": self.SOURCE, "source_url": self.BASE_URL,
                                })
                        except:
                            continue
            
            if not results:
                text = soup.get_text()
                results = self._extract_from_text(text, city, category)
        except Exception as e:
            logger.error(f"RBI scrape error: {e}")
        
        logger.info(f"RBI: Extracted {len(results)} records")
        return results
    
    def _extract_from_text(self, text: str, city: str, category: str) -> List[Dict]:
        results = []
        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        for email in email_pattern.findall(text)[:30]:
            results.append({"name": "RBI Regulated Entity", "email": email, "phone": None,
                           "city": city, "category": category, "source": self.SOURCE})
        return results


class GSTDirectScraper:
    SOURCE = "GST"
    BASE_URL = "https://services.gst.gov.in/services/searchtp"
    API_URL = "https://services.gst.gov.in/services/searchtp"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "GST Practitioner") -> List[Dict]:
        results = []
        logger.info(f"Scraping GST for city={city}")
        
        cities = [city] if city else ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad",
                                       "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
        
        for c in cities:
            try:
                search_url = f"https://services.gst.gov.in/services/searchtp?state={c}"
                html, status = self.fetcher.fetch(search_url, "https://www.google.com/")
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                cards = soup.find_all(['div', 'tr'], class_=lambda x: x and ('practitioner' in str(x).lower() or 'gst' in str(x).lower()))
                
                if not cards:
                    tables = soup.find_all('table')
                    for table in tables:
                        for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                name = cols[0].get_text(strip=True)
                                details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                                if name and len(name) > 3 and "Name" not in name:
                                    email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', details)
                                    phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', details)
                                    results.append({
                                        "name": name[:200],
                                        "email": email_match.group(0) if email_match else None,
                                        "phone": phone_match.group(0) if phone_match else None,
                                        "city": c, "category": category, "source": self.SOURCE, "source_url": search_url,
                                    })
                else:
                    for card in cards:
                        name_elem = card.find(['h3', 'h4', 'strong'])
                        name = name_elem.get_text(strip=True) if name_elem else ""
                        if name and len(name) > 3:
                            text = card.get_text()
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "city": c, "category": category, "source": self.SOURCE, "source_url": search_url,
                            })
            except Exception as e:
                logger.warning(f"GST city {c} error: {e}")
                continue
        
        logger.info(f"GST: Extracted {len(results)} records")
        return results


class ICSIDirectScraper:
    SOURCE = "ICSI"
    BASE_URL = "https://www.icsi.edu/member/icsi-member-directory/"
    SEARCH_URL = "https://www.icsi.edu/member/search-member/"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Company Secretaries") -> List[Dict]:
        results = []
        logger.info(f"Scraping ICSI for city={city}")
        
        cities = [city] if city else ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad",
                                        "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
        
        for c in cities:
            try:
                search_url = f"https://www.icsi.edu/member/search-member/?search={c}"
                html, status = self.fetcher.fetch(search_url, "https://www.google.com/")
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                cards = soup.find_all(['div', 'tr'], class_=lambda x: x and 'member' in str(x).lower())
                
                if not cards:
                    tables = soup.find_all('table')
                    for table in tables:
                        for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                name = cols[0].get_text(strip=True)
                                details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                                if name and len(name) > 3 and "Name" not in name:
                                    email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', details)
                                    phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', details)
                                    results.append({
                                        "name": name[:200],
                                        "email": email_match.group(0) if email_match else None,
                                        "phone": phone_match.group(0) if phone_match else None,
                                        "city": c, "category": category, "source": self.SOURCE,
                                        "source_url": search_url, "membership_no": None,
                                    })
                else:
                    for card in cards:
                        name_elem = card.find(['h3', 'h4', 'strong'])
                        name = name_elem.get_text(strip=True) if name_elem else ""
                        if name and len(name) > 3:
                            text = card.get_text()
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', text)
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "city": c, "category": category, "source": self.SOURCE, "source_url": search_url,
                            })
            except Exception as e:
                logger.warning(f"ICSI city {c} error: {e}")
                continue
        
        logger.info(f"ICSI: Extracted {len(results)} records")
        return results


class IBBIDirectScraper:
    SOURCE = "IBBI"
    BASE_URL = "https://ibbi.gov.in/en/service-provider/insolvency-professionals"
    SEARCH_URL = "https://ibbi.gov.in/en/service-provider/insolvency-professionals"
    
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape(self, city: str = None, category: str = "Insolvency Professionals") -> List[Dict]:
        results = []
        logger.info(f"Scraping IBBI for city={city}")
        
        try:
            html, status = self.fetcher.fetch(self.BASE_URL, "https://www.google.com/")
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        name = cols[0].get_text(strip=True)
                        details = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                        reg_no = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                        if name and len(name) > 3 and "Name" not in name:
                            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', details)
                            phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', details)
                            results.append({
                                "name": name[:200],
                                "email": email_match.group(0) if email_match else None,
                                "phone": phone_match.group(0) if phone_match else None,
                                "address": details[:300], "city": city, "category": category,
                                "source": self.SOURCE, "source_url": self.BASE_URL, "registration_no": reg_no,
                            })
            
            if not results:
                text = soup.get_text()
                email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
                for email in email_pattern.findall(text)[:20]:
                    results.append({"name": "IBBI Insolvency Professional", "email": email, "phone": None,
                                   "city": city, "category": category, "source": self.SOURCE})
        except Exception as e:
            logger.error(f"IBBI scrape error: {e}")
        
        logger.info(f"IBBI: Extracted {len(results)} records")
        return results


class GeneralDirectScraper:
    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()
    
    def scrape_url(self, url: str, referer: str = "https://www.google.com/") -> List[Dict]:
        results = []
        logger.info(f"Direct scraping: {url}")
        
        try:
            html, status = self.fetcher.fetch(url, referer)
            if not html:
                return results
            
            soup = BeautifulSoup(html, 'html.parser')
            emails = set(re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', soup.get_text()))
            phones = set(re.findall(r'(\+91[\s.-]?\d{10}|\b\d{10}\b|\b0\d{10,11}\b)', soup.get_text()))
            
            for email in emails:
                results.append({"name": "Direct Contact", "email": email, "phone": None, "source": "DIRECT", "source_url": url})
            for phone in phones:
                results.append({"name": "Direct Contact", "phone": phone, "email": None, "source": "DIRECT", "source_url": url})
        except Exception as e:
            logger.error(f"Direct scrape error for {url}: {e}")
        
        return results


SCRAPERS = {
    "SEBI": SEBIDirectScraper,
    "ICAI": ICAIDirectScraper,
    "MCA": MCADirectScraper,
    "AMFI": AMFIDirectScraper,
    "NSE": NSEDirectScraper,
    "BCI": BCIDirectScraper,
    "RBI": RBIDirectScraper,
    "GST": GSTDirectScraper,
    "ICSI": ICSIDirectScraper,
    "IBBI": IBBIDirectScraper,
}

def get_scraper(source: str) -> Optional[object]:
    return SCRAPERS.get(source.upper())


if __name__ == "__main__":
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