"""
Direct Scraper - Focused on CA, Mutual Fund, Insurance, Accountant
No proxies - polite HTTP fetching for government/regulatory sites
"""

import re
import time
import random
import logging
import base64
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlencode
from stealth_utils import StealthManager

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    pass

logger = logging.getLogger(__name__)


class DirectScraperConfig:
    MIN_DELAY = 3.0
    MAX_DELAY = 7.0
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
        "Chandigarh", "Surat", "Vadodara", "Nagpur", "Indore",
        "Bhopal", "Patna", "Visakhapatnam", "Coimbatore", "Kochi",
        "Thane", "Pimpri-Chinchwad", "Agra", "Varanasi", "Mysore",
        "Trivandrum", "Rajkot", "Jodhpur", "Raipur", "Dehradun",
    ]

    CA_CONNECT_SERVICES = ["Audit", "Direct Taxes", "Goods and Services Tax"]

    CITY_STATE_MAP = {
        "delhi": "Delhi", "new delhi": "Delhi",
        "mumbai": "Maharashtra", "pune": "Maharashtra", "nagpur": "Maharashtra",
        "bangalore": "Karnataka", "bengaluru": "Karnataka",
        "chennai": "Tamil Nadu", "coimbatore": "Tamil Nadu",
        "hyderabad": "Telangana", "warangal": "Telangana",
        "kolkata": "West Bengal",
        "ahmedabad": "Gujarat", "surat": "Gujarat", "vadodara": "Gujarat", "rajkot": "Gujarat",
        "jaipur": "Rajasthan", "jodhpur": "Rajasthan",
        "lucknow": "Uttar Pradesh", "kanpur": "Uttar Pradesh",
        "noida": "Uttar Pradesh", "ghaziabad": "Uttar Pradesh", "agra": "Uttar Pradesh", "varanasi": "Uttar Pradesh",
        "patna": "Bihar", "indore": "Madhya Pradesh", "bhopal": "Madhya Pradesh",
        "kochi": "Kerala", "trivandrum": "Kerala",
        "chandigarh": "Chandigarh",
        "mysore": "Karnataka",
        "visakhapatnam": "Andhra Pradesh",
        "thiruvananthapuram": "Kerala",
        "pimpri-chinchwad": "Maharashtra",
        "raipur": "Chhattisgarh",
        "dehradun": "Uttarakhand",
    }


class DirectPoliteFetcher:
    def __init__(self, config: DirectScraperConfig = None):
        self.config = config or DirectScraperConfig()
        self.session = requests.Session()
        self._last_request_time = 0
        self._session_ua = StealthManager.get_persistent_ua()

    def _get_random_ua(self) -> str:
        return self._session_ua

    def _get_headers(self, referer: str = "https://www.google.com/") -> Dict:
        ua = self._session_ua
        headers = StealthManager.get_modern_headers(ua)
        if referer:
            headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "same-origin"
        return headers

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


class ICAIDirectScraper:
    SOURCE = "ICAI"
    BASE_URL = "https://www.icai.org/traceamember.html"
    CA_CONNECT_SEARCH_URL = "https://caconnect.icai.org/search"

    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()

    def scrape(self, city: str = None, category: str = "Chartered Accountants") -> List[Dict]:
        results = self._scrape_caconnect(city, category)
        if results:
            logger.info(f"ICAI CA Connect: Extracted {len(results)} records")
            return results

        logger.info(f"ICAI fallback: Scraping for city={city}")
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
                                "city": c, "category": "Chartered Accountants", "source": self.SOURCE,
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


class AMFIDirectScraper:
    SOURCE = "AMFI"
    BASE_URL = "https://www.amfiindia.com/locate-distributor"
    API_URL = "https://www.amfiindia.com/api/distributor-agent"

    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()

    def scrape(self, city: str = None, category: str = "Mutual Fund Agents") -> List[Dict]:
        results = []
        logger.info(f"AMFI: Scraping for city={city}")

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
                    "city": city, "category": "Mutual Fund Agents", "source": self.SOURCE, "source_url": self.BASE_URL
                })
        except Exception as e:
            logger.error(f"AMFI scrape error: {e}")

        logger.info(f"AMFI: Extracted {len(results)} records")
        return results


class SEBIDirectScraper:
    SOURCE = "SEBI"
    BASE_URL = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"

    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()

    def scrape(self, city: str = None, category: str = "Investment Advisors") -> List[Dict]:
        results = []
        logger.info(f"SEBI: Scraping Investment Advisors")

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
                                    "category": "Investment Advisors",
                                    "source": self.SOURCE, "source_url": self.BASE_URL,
                                    "registration_no": reg_no, "license_no": reg_no,
                                })
                        except:
                            continue

            if not results:
                results = self._extract_from_text(soup.get_text(), city, "Investment Advisors")
        except Exception as e:
            logger.error(f"SEBI scrape error: {e}")

        logger.info(f"SEBI: Extracted {len(results)} records")
        return results

    def _extract_from_text(self, text: str, city: str, category: str) -> List[Dict]:
        results = []
        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        phone_pattern = re.compile(r'(\+91[\s.-]?\d{10}|\b\d{10}\b|\b0\d{10,11}\b)')

        for email in email_pattern.findall(text)[:20]:
            results.append({"name": "SEBI Investment Advisor", "email": email, "phone": None,
                           "city": city, "category": category, "source": self.SOURCE})
        for phone in phone_pattern.findall(text)[:20]:
            results.append({"name": "SEBI Investment Advisor", "phone": phone, "email": None,
                           "city": city, "category": category, "source": self.SOURCE})
        return results


class IRDAIDirectScraper:
    SOURCE = "IRDAI"
    BASE_URL = "https://www.irdai.gov.in/page/life-insurance-companies"
    AGENTS_SEARCH_URL = "https://www.irdai.gov.in/page/list-of-agents"

    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()

    def scrape(self, city: str = None, category: str = "Insurance Agents") -> List[Dict]:
        results = []
        logger.info(f"IRDAI: Scraping for city={city}")

        search_urls = [
            "https://www.irdai.gov.in/page/licensed-composite-brokers",
            "https://www.irdai.gov.in/page/licensed-insurance-agents",
            "https://www.irdai.gov.in/page/life-insurance-companies",
            "https://www.irdai.gov.in/page/non-life-insurance-companies",
        ]

        for url in search_urls:
            try:
                html, status = self.fetcher.fetch(url, "https://www.google.com/")
                if not html:
                    continue

                soup = BeautifulSoup(html, 'html.parser')
                tables = soup.find_all('table')

                for table in tables:
                    for row in table.find_all('tr')[1:]:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            name = cols[0].get_text(strip=True)
                            details = cols[1].get_text(strip=True) if len(cols) > 1 else ""

                            if name and len(name) > 3 and "Name" not in name and "S.No" not in name:
                                email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', details)
                                phone_match = re.search(r'(\+91[\s.-]?\d{10}|\b\d{10}\b)', details)

                                results.append({
                                    "name": name[:200],
                                    "email": email_match.group(0) if email_match else None,
                                    "phone": phone_match.group(0) if phone_match else None,
                                    "address": details[:300],
                                    "city": city,
                                    "category": "Insurance Agents",
                                    "source": self.SOURCE,
                                    "source_url": url,
                                })
            except Exception as e:
                logger.warning(f"IRDAI {url} error: {e}")
                continue

        if not results:
            text = soup.get_text() if 'soup' in dir() else ""
            email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
            for email in email_pattern.findall(text)[:30]:
                results.append({
                    "name": "IRDAI Insurance Agent",
                    "email": email, "phone": None,
                    "city": city, "category": "Insurance Agents",
                    "source": self.SOURCE,
                })

        logger.info(f"IRDAI: Extracted {len(results)} records")
        return results


class SchoolDirectScraper:
    SOURCE = "SCHOOL"

    def __init__(self, fetcher: DirectPoliteFetcher = None):
        self.fetcher = fetcher or DirectPoliteFetcher()

    def _decode_bing_url(self, url: str) -> str:
        if not url:
            return ""
        if "bing.com/ck/a" not in url:
            return url
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(url)
            queries = parse_qs(parsed.query)
            u_val = queries.get('u', [None])[0]
            if u_val and len(u_val) > 2:
                # Strip leading "a0", "a1", "a2"
                b64_str = u_val[2:]
                # Add base64 padding
                padding = len(b64_str) % 4
                if padding:
                    b64_str += "=" * (4 - padding)
                decoded = base64.b64decode(b64_str.encode('utf-8')).decode('utf-8', errors='ignore')
                return decoded
        except Exception as e:
            logger.warning(f"Failed to decode Bing redirect URL {url}: {e}")
        return url

    def scrape(self, city: str = None, category: str = "Schools") -> List[Dict]:
        results = []
        # Target regions containing "North" or "North Side" of major Indian cities.
        target_zones = []
        if city:
            city_clean = city.strip()
            if any(x in city_clean.lower() for x in ["north", "south", "east", "west", "side"]):
                target_zones.append(city_clean)
            else:
                target_zones.append(f"North {city_clean}")
                target_zones.append(f"North West {city_clean}")
        else:
            # Default to major Indian cities' North sides
            target_zones = [
                "North Delhi", "North West Delhi",
                "North Bangalore", "North Bengaluru",
                "North Mumbai", "North Kolkata",
                "North Chennai", "North Pune"
            ]

        logger.info(f"SCHOOL SCRAPER: Starting school data extraction for target zones: {target_zones}")

        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        phone_pattern = re.compile(r'(?:\+91[\s.-]?)?\b[6789]\d{9}\b|\b\d{3,5}[\s.-]?\d{6,8}\b')

        seen_domains = set()
        excluded_domains = {
            "grotal.com", "yellowpages.in", "justdial.com", "wikipedia.org", "facebook.com",
            "twitter.com", "instagram.com", "linkedin.com", "youtube.com", "indiamart.com",
            "sulekha.com", "justdial.com", "collegedunia.com", "shiksha.com", "mapsofindia.com",
            "schoolmykids.com", "edustoke.com", "careers360.com", "icbse.com", "schools.org.in"
        }

        for zone in target_zones:
            query = f'schools in "{zone}"'
            encoded_query = urlencode({"q": query})
            
            for page in range(1, 3):
                first = 1 + (page - 1) * 10
                search_url = f"https://www.bing.com/search?{encoded_query}&first={first}"
                logger.info(f"SCHOOL SCRAPER: Querying Bing: {search_url}")
                
                try:
                    html, status = self.fetcher.fetch(search_url, "https://www.bing.com/")
                    if not html or status != 200:
                        logger.warning(f"SCHOOL SCRAPER: Could not fetch search results for zone: {zone}, page: {page} (Status: {status})")
                        break
                        
                    soup = BeautifulSoup(html, 'html.parser')
                    results_list = soup.find_all('li', class_='b_algo')
                    
                    if not results_list:
                        logger.info(f"SCHOOL SCRAPER: No organic search results found on page {page} for zone: {zone}")
                        break
                        
                    for li in results_list:
                        h2 = li.find('h2')
                        if not h2:
                            continue
                        anchor = h2.find('a')
                        if not anchor or not anchor.get('href'):
                            continue
                            
                        school_name = anchor.get_text(strip=True)
                        raw_url = anchor['href']
                        school_url = self._decode_bing_url(raw_url)
                        
                        for separator in [" - ", " | ", " – ", ":"]:
                            if separator in school_name:
                                school_name = school_name.split(separator)[0].strip()
                        
                        from urllib.parse import urlparse
                        parsed_url = urlparse(school_url)
                        domain = parsed_url.netloc.lower()
                        if domain.startswith("www."):
                            domain = domain[4:]
                            
                        snippet_tag = li.find('p') or li.find('div', class_='b_caption') or li.find('div', class_='b_snippet')
                        snippet = snippet_tag.get_text() if snippet_tag else ""
                        
                        emails_found = email_pattern.findall(snippet)
                        phones_found = phone_pattern.findall(snippet)
                        
                        clean_emails = [e for e in emails_found if not any(x in e.lower() for x in ["test", "example", "sample", "domain"])]
                        clean_phones = [p for p in phones_found if len(re.sub(r'\D', '', p)) >= 10]
                        
                        lead = {
                            "name": school_name[:200],
                            "email": clean_emails[0].lower() if clean_emails else None,
                            "phone": clean_phones[0] if clean_phones else None,
                            "address": zone,
                            "city": city or zone.replace("North", "").replace("West", "").strip(),
                            "category": "School",
                            "source": self.SOURCE,
                            "source_url": school_url,
                        }
                        
                        if lead["email"] or lead["phone"]:
                            results.append(lead)
                            logger.info(f"SCHOOL SCRAPER: Found lead from Bing snippet: {school_name} (Email: {lead['email']}, Phone: {lead['phone']})")

                        is_excluded = False
                        if domain:
                            for ex in excluded_domains:
                                if domain == ex or domain.endswith("." + ex):
                                    is_excluded = True
                                    break

                        if domain and not is_excluded and domain not in seen_domains:

                            seen_domains.add(domain)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            contact_paths = ["", "/contact", "/contact-us", "/contactus", "/about-us", "/about"]
                            
                            direct_email = None
                            direct_phone = None
                            
                            for path in contact_paths:
                                target_page = base_url + path
                                logger.info(f"SCHOOL SCRAPER: Politely scanning contact page: {target_page}")
                                
                                try:
                                    page_html, page_status = self.fetcher.fetch(target_page, school_url)
                                    if not page_html or page_status != 200:
                                        continue
                                        
                                    page_soup = BeautifulSoup(page_html, 'html.parser')
                                    for script in page_soup(["script", "style"]):
                                        script.decompose()
                                    text_content = page_soup.get_text()
                                    
                                    page_emails = email_pattern.findall(text_content)
                                    page_phones = phone_pattern.findall(text_content)
                                    
                                    valid_emails = [e for e in page_emails if not any(x in e.lower() for x in ["test", "example", "sample", "domain"])]
                                    valid_phones = [p for p in page_phones if len(re.sub(r'\D', '', p)) >= 10]
                                    
                                    if valid_emails:
                                        direct_email = valid_emails[0].lower()
                                    if valid_phones:
                                        raw_digits = re.sub(r'\D', '', valid_phones[0])
                                        direct_phone = raw_digits[-10:] if len(raw_digits) >= 10 else raw_digits
                                        
                                    if direct_email or direct_phone:
                                        break
                                except Exception as crawl_err:
                                    logger.warning(f"SCHOOL SCRAPER: Direct crawl failed for {target_page}: {crawl_err}")
                                    continue
                            
                            if direct_email or direct_phone:
                                direct_lead = {
                                    "name": school_name[:200],
                                    "email": direct_email,
                                    "phone": direct_phone,
                                    "address": zone,
                                    "city": city or zone.replace("North", "").replace("West", "").strip(),
                                    "category": "School",
                                    "source": self.SOURCE,
                                    "source_url": base_url,
                                }
                                results.append(direct_lead)
                                logger.info(f"SCHOOL SCRAPER: Successfully extracted direct website lead: {school_name} (Email: {direct_email}, Phone: {direct_phone})")
                                
                except Exception as page_err:
                    logger.error(f"SCHOOL SCRAPER: Error scanning zone {zone}, page {page}: {page_err}")
                    continue
                    
        unique_results = []
        seen_leads = set()
        for r in results:
            if not r.get("phone") and not r.get("email"):
                continue
            lead_key = (r["name"].lower(), r.get("email"), r.get("phone"))
            if lead_key not in seen_leads:
                seen_leads.add(lead_key)
                unique_results.append(r)
                
        logger.info(f"SCHOOL SCRAPER: School scraping completed! Extracted {len(unique_results)} unique records.")
        return unique_results


# Registry
SCRAPERS = {
    "ICAI": ICAIDirectScraper,
    "AMFI": AMFIDirectScraper,
    "SEBI": SEBIDirectScraper,
    "IRDAI": IRDAIDirectScraper,
    "SCHOOL": SchoolDirectScraper,
}


def get_scraper(source: str) -> Optional[object]:
    return SCRAPERS.get(source.upper())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetcher = DirectPoliteFetcher()

    print("\n=== Testing ICAI Direct Scraper ===")
    scraper = ICAIDirectScraper(fetcher)
    results = scraper.scrape(city="Delhi", category="Chartered Accountants")
    print(f"ICAI Results: {len(results)} records")

    print("\n=== Testing School Direct Scraper ===")
    scraper = SchoolDirectScraper(fetcher)
    results = scraper.scrape(city="Delhi", category="Schools")
    print(f"School Results: {len(results)} records")