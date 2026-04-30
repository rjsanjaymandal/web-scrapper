import logging
import re
import asyncio
import json
import aiohttp
from typing import List, Dict, Optional
from scrapers.base import BaseScraper, ScraperRegistry

logger = logging.getLogger(__name__)

class AMFIScraper(BaseScraper):
    """Scraper for AMFI - Mutual Fund Agents"""
    source_name = "AMFI"
    BASE_URL = "https://www.amfiindia.com/locate-distributor"
    SEARCH_API_URL = "https://www.amfiindia.com/api/locate-distributor"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    def get_search_params(self, city: str, page: int = 1, page_size: int = 100) -> Dict:
        """API Parameters for AMFI extraction"""
        return {
            "city": city,
            "page": page,
            "size": page_size
        }

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        # Implementation is handled via OfficialAPIHandlers.handle_amfi high-speed method
        return self.extract_raw_fallback(html_content, city, category)

class IRDAIScraper(BaseScraper):
    """Scraper for IRDAI - Insurance Agents"""
    source_name = "IRDAI"
    BASE_URL = "https://www.irdai.gov.in/ADMINCMS/cms/NormalData_Layout.aspx?page=PageNo225"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class ICAIScraper(BaseScraper):
    """Scraper for ICAI - Chartered Accountants"""
    source_name = "ICAI"
    BASE_URL = "https://www.icai.org/traceamember.html"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class ICSIScraper(BaseScraper):
    """Scraper for ICSI - Company Secretaries"""
    source_name = "ICSI"
    BASE_URL = "https://www.icsi.edu/member/icsi-member-directory/"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class SEBIScraper(BaseScraper):
    """Specialized scraper for SEBI Registered Investment Advisors and Intermediaries."""
    source_name = "SEBI"
    SEARCH_URL = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.SEARCH_URL
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            from bs4 import BeautifulSoup
            if html_content:
                content = html_content
            elif page and hasattr(page, 'content'):
                content = await page.content()
            else:
                content = ""
            
            if not content: return []
            
            soup = BeautifulSoup(content, 'lxml')
            table = soup.select_one('table#sample_1, .table-striped, table[border="1"]')
            
            if table:
                rows = table.select('tr')
                for row in rows:
                    cols = row.select('td')
                    if len(cols) >= 3:
                        name = cols[1].get_text(strip=True)
                        reg_no = cols[0].get_text(strip=True)
                        addr = cols[2].get_text(strip=True)
                        
                        if name and "Name" not in name:
                            listings.append({
                                'name': name[:150],
                                'registration_no': reg_no,
                                'address': addr[:200],
                                'city': city,
                                'source': 'SEBI'
                            })
            
            if not listings:
                listings = self.extract_raw_fallback(content, city, category)
        except Exception as e:
            logger.error(f"SEBI Scraper Error: {e}")
        return listings

class IBBIScraper(BaseScraper):
    """Scraper for IBBI - Insolvency Professionals"""
    source_name = "IBBI"
    BASE_URL = "https://ibbi.gov.in/en/service-provider/insolvency-professionals"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class NSEScraper(BaseScraper):
    """Specialized scraper for NSE Authorized Persons."""
    source_name = "NSE"
    SEARCH_URL = "https://www.nseindia.com/members/content/member_directory.htm"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.SEARCH_URL
    
    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            from bs4 import BeautifulSoup
            if html_content:
                content = html_content
            elif page and hasattr(page, 'content'):
                content = await page.content()
            else:
                content = ""
            
            if not content: return []
            
            soup = BeautifulSoup(content, 'lxml')
            table = soup.select_one('table#memberDirectoryTable, .common_table')
            if table:
                rows = table.select('tr')[1:]
                for row in rows:
                    cols = row.select('td')
                    if len(cols) >= 4:
                        listings.append({
                            'name': cols[1].get_text(strip=True),
                            'address': cols[3].get_text(strip=True),
                            'source_id': cols[0].get_text(strip=True),
                            'city': city,
                            'source': 'NSE'
                        })
        except Exception as e:
            logger.warning(f"NSE extraction error: {e}")
        return listings

class BSEScraper(BaseScraper):
    """Scraper for BSE Brokers"""
    source_name = "BSE"
    BASE_URL = "https://www.bseindia.com/members/MemberDirectory.aspx"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class GSTPractitionerScraper(BaseScraper):
    """Scraper for GST Practitioners"""
    source_name = "GST"
    BASE_URL = "https://services.gst.gov.in/services/searchtp"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class RBIRegulatedScraper(BaseScraper):
    """Scraper for RBI Regulated NBFCs and Entities"""
    source_name = "RBI"
    BASE_URL = "https://www.rbi.org.in/Scripts/BS_NBFCList.aspx"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class BarCouncilScraper(BaseScraper):
    """Scraper for Bar Councils - Lawyers and Advocates"""
    source_name = "BAR_COUNCIL"
    BASE_URL = "https://www.indianlawyer.info/directory"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.BASE_URL

    async def extract_listings(self, page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        # Implementation handled via OfficialAPIHandlers.handle_bar_council
        return self.extract_raw_fallback(html_content, city, category)

# Register all official scrapers
ScraperRegistry.register(AMFIScraper)
ScraperRegistry.register(IRDAIScraper)
ScraperRegistry.register(ICAIScraper)
ScraperRegistry.register(ICSIScraper)
ScraperRegistry.register(SEBIScraper)
ScraperRegistry.register(IBBIScraper)
ScraperRegistry.register(NSEScraper)
ScraperRegistry.register(BSEScraper)
ScraperRegistry.register(GSTPractitionerScraper)
ScraperRegistry.register(RBIRegulatedScraper)
ScraperRegistry.register(BarCouncilScraper)
