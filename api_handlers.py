"""
API Handlers for Official Registries
High-speed extraction without Playwright/Browser.
Targets: AMFI, SEBI, IBBI, Bar Council, ICAI, IRDAI.
"""
import logging
import re
import json
from typing import List, Dict, Optional, Callable, Awaitable
from polite_http_scraper import PoliteHTTPScraper

from scrapers.base import BaseScraper
logger = logging.getLogger(__name__)

class OfficialAPIHandlers:
    """Specialized handlers for each regulatory body"""
    
    @classmethod
    def get_handler(
        cls, source: str, category: Optional[str] = None
    ) -> Optional[Callable[[PoliteHTTPScraper, str], Awaitable[List[Dict]]]]:
        """Resolve handlers lazily so a missing method only disables one source."""
        key = (source or "").upper()
        category_name = category or "business"

        direct_handlers = {
            "AMFI": "handle_amfi",
            "SEBI": "handle_sebi_ria",
            "IBBI": "handle_ibbi_insolvency",
            "BAR_COUNCIL": "handle_bar_council",
            "ICAI": "handle_icai",
            "ICSI": "handle_icsi",
            "GST": "handle_gst",
            "IRDAI": "handle_irdai",
            "YELLOWPAGES": "handle_yellowpages",
            "JUSTDIAL": "handle_justdial_fallback",
        }

        sitemap_sources = {
            "SITEMAP",
            "EXPORTERSINDIA",
            "ASKLAILA",
            "VYKARI",
            "CLICKINDIA",
            "GROTAL",
            "NSE",
            "BSE",
            "RBI",
        }

        if key in sitemap_sources:
            method = getattr(cls, "handle_sitemap", None)
            if not method:
                return None

            async def _handler(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
                return await method(engine, city, key, category_name)

            return _handler

        method_name = direct_handlers.get(key)
        if not method_name:
            return None

        handler = getattr(cls, method_name, None)
        if not handler:
            logger.warning(
                "Handler %s for source %s is not available; skipping source.",
                method_name,
                key,
            )
            return None

        if key in {"YELLOWPAGES", "JUSTDIAL"}:
            async def _handler(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
                return await handler(engine, city, category_name)

            return _handler

        return handler

    @classmethod
    def supports(cls, source: str, category: Optional[str] = None) -> bool:
        return cls.get_handler(source, category) is not None
    
    @staticmethod
    async def handle_amfi(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Mutual Fund Distributors from AMFI (2026 API)"""
        # The verified 2026 API endpoint for distributor lookups
        url = "https://www.amfiindia.com/api/distributor-agent"
        # API-specific headers for 2026 registry
        custom_headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.amfiindia.com/locate-distributor",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        params = {
            "strOpt": "ALL",
            "city": city.title(),
            "search": "",
            "page": 1,
            "pageSize": 100
        }
        
        response = await engine.fetch(url, method="GET", params=params, headers=custom_headers, is_json_api=True)
        if not response or response.status != 200:
            logger.warning(f"AMFI API returned status {response.status if response else 'None'}")
            return []
            
        try:
            data = await response.json()
            leads = []
            # AMFI API usually returns a list of objects in a 'data' or 'list' field
            items = []
            if isinstance(data, dict):
                items = data.get("data") or data.get("list") or []
            elif isinstance(data, list):
                items = data
            
            for item in items:
                leads.append({
                    "name": item.get("ARNHolderName") or item.get("name") or item.get("distributor_name"),
                    "arn": item.get("ARN") or item.get("arn_number") or item.get("arn"),
                    "phone": item.get("TelephoneNumber_O") or item.get("mobile_number") or item.get("phone"),
                    "email": item.get("Email") or item.get("email"),
                    "address": item.get("Address") or item.get("address"),
                    "city": item.get("City") or city,
                    "source": "AMFI",
                    "category": "Mutual Fund"
                })
            return leads
        except Exception as e:
            logger.error(f"Failed to parse AMFI JSON response: {e}")
            # Fallback to HTML if API failed
            html_url = "https://www.amfiindia.com/locate-distributor"
            resp = await engine.fetch(html_url)
            if resp:
                html = await resp.text()
                return BaseScraper.extract_raw_fallback(html, city, "Mutual Fund")
            return []

    @staticmethod
    async def handle_irdai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insurance Agents from IRDAI Agent Locator (2026 API)"""
        url = "https://agencyportal.irdai.gov.in/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent"
        
        # Mapping for common cities to State/District codes (can be expanded)
        # 6: Gujarat, 102: Ahmedabad
        city_mapping = {
            "AHMEDABAD": ("6", "102"),
            "MUMBAI": ("15", "257"),
            "DELHI": ("5", "94"),
            "BANGALORE": ("12", "195"),
            "HYDERABAD": ("1", "1"),
            "CHENNAI": ("22", "390"),
            "PUNE": ("15", "273"),
            "KOLKATA": ("28", "493"),
            "SURAT": ("6", "106"),
            "JAIPUR": ("20", "349"),
            "LUCKNOW": ("25", "441"),
            "KANPUR": ("25", "436"),
            "NAGPUR": ("15", "270"),
            "INDORE": ("14", "229"),
            "THANE": ("15", "280"),
            "BHOPAL": ("14", "221"),
            "PATNA": ("4", "58"),
            "VADODARA": ("6", "108"),
            "ALLAHABAD": ("25", "444"),
            "PRAYAGRAJ": ("25", "444"),
            "UDAIPUR": ("20", "354")
        }
        
        state_id, district_id = city_mapping.get(city.upper(), ("", ""))
        
        # IRDAI requires Insurance Type (1: General, 2: Life, 3: Health) and Insurer ID
        # Since we want all, we might need a loop, but we'll try a common one first (General/Bajaj: 1,8)
        # or Life/LIC: 2, 21
        custom_query = f",,,1,8,{state_id},{district_id},"
        
        payload = {
            "page": 1,
            "rp": 500,
            "sortname": "AgentName",
            "sortorder": "asc",
            "query": "",
            "qtype": "",
            "customquery": custom_query
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "*/*"
        }
        
        response = await engine.fetch(url, method="POST", data=payload, headers=headers)
        if not response:
            # Fallback to main portal if API fails
            logger.info("IRDAI API failed, falling back to main portal search")
            fallback_url = f"https://www.irdai.gov.in/Defaul3.aspx?page=agent_locator&city={city}"
            response = await engine.fetch(fallback_url)
            if not response: return []
            
        try:
            html_text = await response.text()
            leads = []
            
            # The API returns XML/HTML wrapped in a string or direct XML
            # If flexigrid, it might be JSON in some versions, but subagent saw XML-like data
            
            # Extract names, phones, emails using regex for speed and robustness
            cells = re.findall(r'<cell>(.*?)</cell>', html_text)
            # Group into records (flexigrid returns 16 cells per row in 2026)
            record_size = 16
            for i in range(0, len(cells), record_size):
                chunk = cells[i:i+record_size]
                if len(chunk) >= 15:
                    leads.append({
                        "name": chunk[1].strip(),
                        "license_no": chunk[2].strip(),
                        "address": f"{chunk[8].strip()}, {chunk[9].strip()} {chunk[10].strip()}",
                        "phone": chunk[14].strip() or chunk[15].strip(),
                        "email": None, # IRDAI API often hides email in this view
                        "city": chunk[9].strip() or city,
                        "source": "IRDAI",
                        "category": "Insurance"
                    })
            
            return leads
        except Exception as e:
            logger.error(f"Failed to parse IRDAI response: {e}")
            return []

    @staticmethod
    async def handle_sebi_ria(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch SEBI Registered Investment Advisors (High Speed)"""
        url = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"
        # In a real scenario, this would involve a POST with params, 
        # but here we'll use the basic fetch + BS4 logic from official.py
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        table = soup.select_one('table#sample_1, .table-striped, table[border="1"]')
        leads = []
        if table:
            rows = table.select('tr')
            for row in rows:
                cols = row.select('td')
                if len(cols) >= 3:
                    name = cols[1].get_text(strip=True)
                    if name and "Name" not in name:
                        leads.append({
                            "name": name,
                            "registration_no": cols[0].get_text(strip=True),
                            "address": cols[2].get_text(strip=True),
                            "city": city,
                            "source": "SEBI",
                            "category": "Investment Advisor"
                        })
        return leads if leads else BaseScraper.extract_raw_fallback(html, city, "Investment Advisor")

    @staticmethod
    async def handle_ibbi_insolvency(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch IBBI Insolvency Professionals"""
        url = "https://ibbi.gov.in/en/service-provider/insolvency-professionals"
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        return BaseScraper.extract_raw_fallback(html, city, "Insolvency Professional")

    @staticmethod
    async def handle_bar_council(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Lawyers from Bar Council directory"""
        url = "https://www.indianlawyer.info/directory"
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        return BaseScraper.extract_raw_fallback(html, city, "Lawyer")

    @staticmethod
    async def handle_icai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Chartered Accountants from ICAI"""
        url = "https://www.icai.org/traceamember.html"
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        return BaseScraper.extract_raw_fallback(html, city, "Chartered Accountant")

    @staticmethod
    async def handle_icsi(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Company Secretaries from ICSI"""
        url = "https://www.icsi.edu/member/icsi-member-directory/"
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        return BaseScraper.extract_raw_fallback(html, city, "Company Secretary")

    @staticmethod
    async def handle_gst(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch GST Practitioners"""
        url = "https://services.gst.gov.in/services/searchtp"
        resp = await engine.fetch(url)
        if not resp: return []
        html = await resp.text()
        return BaseScraper.extract_raw_fallback(html, city, "GST Practitioner")

    @staticmethod
    async def handle_sitemap(
        engine: PoliteHTTPScraper,
        city: str,
        source: str,
        category: str = "business",
    ) -> List[Dict]:
        """Generic sitemap/directory extractor for high-volume directories."""
        from scrapers.base import ScraperRegistry
        scraper = ScraperRegistry.get(source)
        if not scraper:
            return []
        
        url = scraper.build_search_url(city, category)
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, category, html)
        return []

    @staticmethod
    async def handle_yellowpages(
        engine: PoliteHTTPScraper, city: str, category: str = "business"
    ) -> List[Dict]:
        """Fetch from YellowPages India (Stable HTTP target)"""
        from scrapers.directory import YellowPagesIndiaScraper
        scraper = YellowPagesIndiaScraper()
        url = scraper.build_search_url(city, category)
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, category, html)
        return []

    @staticmethod
    async def handle_justdial_fallback(
        engine: PoliteHTTPScraper, city: str, category: str = "business"
    ) -> List[Dict]:
        """Lightweight JustDial extraction attempt via HTTP (Often WAF blocked)"""
        from scrapers.business import JustDialScraper
        scraper = JustDialScraper()
        url = scraper.build_search_url(city, category)
        # JD is very sensitive, we use a more aggressive delay if possible or just try once
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, category, html)
        return []

    @classmethod
    async def dispatch(
        cls,
        source: str,
        engine: PoliteHTTPScraper,
        city: str,
        category: Optional[str] = None,
    ) -> List[Dict]:
        """Routes to the correct handler based on source name"""
        handler = cls.get_handler(source, category)
        if not handler:
            logger.info("Skipping %s: no fast HTTP handler registered.", source)
            return []

        try:
            return await handler(engine, city)
        except Exception as e:
            logger.error(f"Error in API handler for {source}: {e}")
            # Don't re-raise, return empty to allow other scrapers to continue
            
        return []
