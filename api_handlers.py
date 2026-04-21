"""
API Handlers for Official Registries
High-speed extraction without Playwright/Browser.
Targets: SEBI, IBBI, Bar Council, Regional CAs.
"""
import logging
import re
from typing import List, Dict, Optional
from fast_scraper import FastHTTPScraper

logger = logging.getLogger(__name__)

class OfficialAPIHandlers:
    """Specialized handlers for each regulatory body"""
    
    @staticmethod
    async def handle_sebi_ria(engine: FastHTTPScraper, city: str) -> List[Dict]:
        """Fetch Registered Investment Advisors from SEBI"""
        # SEBI Search Endpoint
        url = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do"
        params = {
            "doRegistrants": "yes",
            "intmId": "13", # 13 = Registered Investment Advisor
            "city": city.title()
        }
        
        leads_data = await engine.scrape_json_api(url, params=params)
        
        leads = []
        for r in leads_data:
            leads.append({
                "name": r.get("name") or r.get("Name"),
                "phone": r.get("contact_no") or r.get("Mobile"),
                "email": r.get("email_id") or r.get("Email"),
                "address": r.get("reg_address") or r.get("Address"),
                "source": "SEBI",
                "city": city,
                "license_no": r.get("reg_no")
            })
        return leads

    @staticmethod
    async def handle_ibbi_insolvency(engine: FastHTTPScraper, city: str) -> List[Dict]:
        """Fetch Insolvency Professionals from IBBI"""
        url = "https://ibbi.gov.in/en/insolvency-professional/export-data-json"
        # IBBI usually accepts city/state filter in JSON payload or params
        params = {"city": city.title()}
        
        leads_data = await engine.scrape_json_api(url, params=params)
        
        leads = []
        for r in leads_data:
            leads.append({
                "name": r.get("name"),
                "phone": r.get("mobile"),
                "email": r.get("email"),
                "address": r.get("address"),
                "source": "IBBI",
                "city": city,
                "license_no": r.get("registration_number")
            })
        return leads

    @staticmethod
    async def handle_bar_council(engine: FastHTTPScraper, city: str) -> List[Dict]:
        """Fetch Lawyer directories via sitemap or listing pages"""
        # Example: Bar Council of Maharashtra & Goa
        # Many state bar councils use public listings.
        # This is a placeholder for a sitemap-based extraction patterns.
        base_url = "https://www.barcouncilmahgoa.org/advocate-directory"
        # Direct fetch of the directory page
        # In actual implementation, we might parse the table using BeautifulSoup
        return [] # Placeholder

    @classmethod
    async def dispatch(cls, source: str, engine: FastHTTPScraper, city: str) -> List[Dict]:
        """Routes to the correct handler based on source name"""
        handlers = {
            "SEBI": cls.handle_sebi_ria,
            "IBBI": cls.handle_ibbi_insolvency,
            "BAR_COUNCIL": cls.handle_bar_council
        }
        
        handler = handlers.get(source)
        if handler:
            return await handler(engine, city)
        return []
