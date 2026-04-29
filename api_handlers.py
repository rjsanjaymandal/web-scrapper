"""
API Handlers for Official Registries
High-speed extraction without Playwright/Browser.
Targets: AMFI, SEBI, IBBI, Bar Council, ICAI, IRDAI.
"""
import logging
import re
import json
from typing import List, Dict, Optional
from polite_http_scraper import PoliteHTTPScraper

logger = logging.getLogger(__name__)

class OfficialAPIHandlers:
    """Specialized handlers for each regulatory body"""
    
    @staticmethod
    async def handle_amfi(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Mutual Fund Distributors from AMFI"""
        url = "https://www.amfiindia.com/locate-your-nearest-mutual-fund-distributor"
        # AMFI uses a POST request for searches
        payload = {
            "city": city,
            "distributor": "",
            "arn": ""
        }
        # In 2026, many of these are behind a simple CSRF or session. 
        # PoliteHTTPScraper handles cookies.
        response = await engine.fetch(url, method="POST", data=payload)
        if not response:
            return []
            
        html = await response.text()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        
        leads = []
        table = soup.select_one('table#distributorTable, .dist-list')
        if not table:
            # Fallback to regex extraction
            from scrapers.base import BaseScraper
            return BaseScraper().extract_raw_fallback(html, city, "Mutual Fund")
            
        rows = table.select('tr')[1:] 
        for row in rows:
            cols = row.select('td')
            if len(cols) >= 4:
                leads.append({
                    "name": cols[0].get_text(strip=True),
                    "arn": cols[1].get_text(strip=True),
                    "phone": cols[3].get_text(strip=True),
                    "city": city,
                    "source": "AMFI",
                    "category": "Mutual Fund"
                })
        return leads

    @staticmethod
    async def handle_sebi_ria(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Registered Investment Advisors from SEBI"""
        url = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes&intmId=13"
        response = await engine.fetch(url, method="GET")
        if not response:
            return []
            
        html = await response.text()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        
        leads = []
        table = soup.select_one('table#sample_1, .table-striped')
        if not table:
            return []
            
        rows = table.select('tr')[1:]
        for row in rows:
            cols = row.select('td')
            if len(cols) >= 3:
                leads.append({
                    "name": cols[1].get_text(strip=True),
                    "address": cols[2].get_text(strip=True),
                    "source": "SEBI",
                    "city": city,
                    "category": "Investment Advisor"
                })
        return leads

    @staticmethod
    async def handle_ibbi_insolvency(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insolvency Professionals from IBBI"""
        url = "https://ibbi.gov.in/en/service-provider/insolvency-professionals"
        # IBBI often has a JSON export or a searchable table
        response = await engine.fetch(url, method="GET")
        if not response:
            return []
            
        html = await response.text()
        from scrapers.base import BaseScraper
        # IPs have strict licensing, so we use high-fidelity regex if table fails
        return BaseScraper().extract_raw_fallback(html, city, "Insolvency Professional")

    @staticmethod
    async def handle_bar_council(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Lawyers from Bar Councils"""
        # We target a consolidated directory or major state councils
        url = f"https://www.indianlawyer.info/directory?city={city}"
        response = await engine.fetch(url, method="GET")
        if not response:
            return []
            
        html = await response.text()
        from scrapers.base import BaseScraper
        return BaseScraper().extract_raw_fallback(html, city, "Lawyer")

    @staticmethod
    async def handle_icai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Chartered Accountants from ICAI"""
        # ICAI Trace is the 2026 standard for finding CAs
        url = f"https://trace.icai.org/trace/trace_search.php?city={city}"
        response = await engine.fetch(url, method="GET")
        if not response:
            return []
        
        html = await response.text()
        from scrapers.base import BaseScraper
        return BaseScraper().extract_raw_fallback(html, city, "Chartered Accountant")

    @staticmethod
    async def handle_irdai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insurance Agents from IRDAI"""
        url = "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx"
        # IRDAI usually requires a POST with ViewState. 
        # We'll use a polite fetch to establish session then attempt extraction.
        response = await engine.fetch(url, method="GET")
        if not response: return []
        
        html = await response.text()
        from scrapers.base import BaseScraper
        return BaseScraper().extract_raw_fallback(html, city, "Insurance Agent")

    @staticmethod
    async def handle_sitemap(engine: PoliteHTTPScraper, city: str, source: str) -> List[Dict]:
        """Generic sitemap/directory extractor for high-volume directories."""
        from scrapers.base import ScraperRegistry
        scraper_cls = ScraperRegistry.get_scraper(source)
        if not scraper_cls:
            return []
        
        scraper = scraper_cls()
        if source == "SITEMAP":
            return await scraper.extract_listings(None, city, "business", None)
            
        url = scraper.build_search_url(city, "business")
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, "business", html)
        return []

    @staticmethod
    async def handle_yellowpages(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch from YellowPages India (Stable HTTP target)"""
        from scrapers.directory import YellowPagesIndiaScraper
        scraper = YellowPagesIndiaScraper()
        url = scraper.build_search_url(city, "business")
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, "business", html)
        return []

    @staticmethod
    async def handle_justdial_fallback(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Lightweight JustDial extraction attempt via HTTP (Often WAF blocked)"""
        from scrapers.business import JustDialScraper
        scraper = JustDialScraper()
        url = scraper.build_search_url(city, "business")
        # JD is very sensitive, we use a more aggressive delay if possible or just try once
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, "business", html)
        return []

    @classmethod
    async def dispatch(cls, source: str, engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Routes to the correct handler based on source name"""
        handlers = {
            "AMFI": cls.handle_amfi,
            "SEBI": cls.handle_sebi_ria,
            "IBBI": cls.handle_ibbi_insolvency,
            "BAR_COUNCIL": cls.handle_bar_council,
            "ICAI": cls.handle_icai,
            "IRDAI": cls.handle_irdai,
            "SITEMAP": lambda e, c: cls.handle_sitemap(e, c, "SITEMAP"),
            "EXPORTERSINDIA": lambda e, c: cls.handle_sitemap(e, c, "EXPORTERSINDIA"),
            "ASKLAILA": lambda e, c: cls.handle_sitemap(e, c, "ASKLAILA"),
            "VYKARI": lambda e, c: cls.handle_sitemap(e, c, "VYKARI"),
            "YELLOWPAGES": cls.handle_yellowpages,
            "JUSTDIAL": cls.handle_justdial_fallback
        }
        
        handler = handlers.get(source.upper())
        if handler:
            try:
                return await handler(engine, city)
            except Exception as e:
                logger.error(f"Error in API handler for {source}: {e}")
        return []
