"""
API Handlers for Official Registries
High-speed extraction without Playwright/Browser.
Targets: SEBI, IBBI, Bar Council, Regional CAs.
"""
import logging
import re
from typing import List, Dict, Optional
from polite_http_scraper import PoliteHTTPScraper

logger = logging.getLogger(__name__)

class OfficialAPIHandlers:
    """Specialized handlers for each regulatory body"""
    
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
            
        rows = table.select('tr')[1:] # Skip header
        for row in rows:
            cols = row.select('td')
            if len(cols) >= 3:
                name = cols[1].get_text(strip=True)
                addr = cols[2].get_text(strip=True)
                # SEBI usually requires clicking for detail, but we can extract what's there
                leads.append({
                    "name": name,
                    "address": addr,
                    "source": "SEBI",
                    "city": city,
                    "category": "sebi-advisor"
                })
        return leads

    @staticmethod
    async def handle_ibbi_insolvency(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insolvency Professionals from IBBI"""
        # Official IP Registry JSON Endpoint
        url = "https://ibbi.gov.in/en/insolvency-professional/export-data-json"
        response = await engine.fetch(url, method="GET")
        if not response:
            return []
            
        try:
            data = await response.json(content_type=None)
            leads_data = data if isinstance(data, list) else data.get("data", [])
        except:
            return []
            
        leads = []
        for r in leads_data:
            # Filter by city if possible (Support "all" for bulk draining)
            r_city = str(r.get("city", "")).lower()
            if city.lower() != "all" and city.lower() not in r_city and r_city:
                continue
                
            leads.append({
                "name": r.get("name") or r.get("Name"),
                "phone": r.get("mobile") or r.get("Phone"),
                "email": r.get("email") or r.get("Email"),
                "address": r.get("address") or r.get("Address"),
                "source": "IBBI",
                "city": city,
                "license_no": r.get("registration_number") or r.get("RegNo")
            })
        return leads

    @staticmethod
    async def handle_bar_council(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Lawyer directories via sitemap or listing pages"""
        # Example: Bar Council of Maharashtra & Goa
        # Many state bar councils use public listings.
        # This is a placeholder for a sitemap-based extraction patterns.
        base_url = "https://www.barcouncilmahgoa.org/advocate-directory"
        # Direct fetch of the directory page
        # In actual implementation, we might parse the table using BeautifulSoup
        return [] # Placeholder

    @staticmethod
    async def handle_icai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Chartered Accountants from ICAI"""
        # Official Search Endpoint for 2026:
        # url = "https://www.icai.org/post.html?post_id=538"
        # Logic to be implemented: ASP.NET postback parsing
        return []

    @staticmethod
    async def handle_irdai(engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insurance Agents from IRDAI"""
        # Official Search Endpoint for 2026:
        # url = "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx"
        # Logic to be implemented: ViewState extraction and Form submission
        return []

    @staticmethod
    async def handle_sitemap(engine: PoliteHTTPScraper, city: str, source: str) -> List[Dict]:
        """Generic sitemap/directory extractor for high-volume directories."""
        from scrapers_registry import ScraperRegistry
        scraper = ScraperRegistry.get(source)
        if not scraper:
            return []
        
        # If it's the dedicated SitemapScraper, it handles its own multi-link fetching
        if source == "SITEMAP":
            return await scraper.extract_listings(None, city, "business", None)
            
        # For others (ExportersIndia, etc.), we fetch the search page once and use fallback
        url = scraper.build_search_url(city, "business")
        resp = await engine.fetch(url)
        if resp and resp.status == 200:
            html = await resp.text()
            return await scraper.extract_listings(None, city, "business", html)
        return []

    @classmethod
    async def dispatch(cls, source: str, engine: PoliteHTTPScraper, city: str) -> List[Dict]:
        """Routes to the correct handler based on source name"""
        handlers = {
            "SEBI": cls.handle_sebi_ria,
            "IBBI": cls.handle_ibbi_insolvency,
            "BAR_COUNCIL": cls.handle_bar_council,
            "ICAI": cls.handle_icai,
            "IRDAI": cls.handle_irdai,
            "SITEMAP": lambda e, c: cls.handle_sitemap(e, c, "SITEMAP"),
            "EXPORTERSINDIA": lambda e, c: cls.handle_sitemap(e, c, "EXPORTERSINDIA"),
            "ASKLAILA": lambda e, c: cls.handle_sitemap(e, c, "ASKLAILA"),
            "VYKARI": lambda e, c: cls.handle_sitemap(e, c, "VYKARI")
        }
        
        handler = handlers.get(source)
        if handler:
            return await handler(engine, city)
        return []
