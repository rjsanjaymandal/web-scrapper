"""
Enhanced Scraper Utilities Module
Additional scrapers, validation, and utilities for more efficient data collection
"""

import asyncio
import logging
import re
import aiohttp
import hashlib
from typing import Optional, Dict, List
from datetime import datetime
from scrapers.directory import SitemapScraper
# from playwright.async_api import Page
Page = dict
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

try:
    from scrapers.base import BaseScraper, ScraperRegistry
except ImportError:
    BaseScraper = object
    class ScraperRegistry:
        @staticmethod
        def get_scraper(name):
            return None

try:
    from data_quality import DataQualityHandler
except ImportError:
    DataQualityHandler = None

# ==================== Additional Scrapers ====================

class SulekhaScraper(BaseScraper):
    source_name = "SULEKHA"
    
    CATEGORY_MAP = {
        'Insurance-Agents': 'insurance-agents',
        'Mutual-Fund-Agents': 'mutual-fund-agents',
        'Tax-Advocates': 'tax-advocates-ca'
    }
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = self.CATEGORY_MAP.get(category, category.lower().replace(' ', '-'))
        return f"https://www.sulekha.com/local/{cat}/{city.lower()}"
    
    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('a.business-name')
            if link:
                return await link.get_attribute('href')
        except:
            pass
        return None
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            if html_content:
                # Use BeautifulSoup for faster/cleaner parsing from raw HTML
                soup = BeautifulSoup(html_content, 'lxml')
                cards = soup.select('.business-card, .listing-item, div[data-listing]')
                for card in cards:
                    try:
                        name = card.select_one('.business-name, .title, h3')
                        phone = card.select_one('.phone, .contact-phone, .contact-number')
                        addr = card.select_one('.address, .location, span.loc')
                        link = card.select_one('a.business-name, .title a')
                        
                        if name:
                            listings.append({
                                'name': name.get_text(strip=True),
                                'phone': self._clean_phone(phone.get_text(strip=True)) if phone else None,
                                'address': addr.get_text(strip=True) if addr else None,
                                'detail_url': link.get('href') if link else None
                            })
                    except:
                        continue
            else:
                # Fallback to Playwright if no HTML content provided
                await page.wait_for_selector('.business-list, .search-results', timeout=10000)
                cards = await page.query_selector_all('.business-card, .listing-item')
                for card in cards:
                    try:
                        name = await self._get_text(card, '.business-name, .title')
                        phone = await self._get_text(card, '.phone, .contact-phone')
                        address = await self._get_text(card, '.address, .location')
                        detail_url = await self.get_detail_url(card)
                        if name:
                            listings.append({
                                'name': name.strip(),
                                'phone': self._clean_phone(phone),
                                'address': address.strip() if address else None,
                                'detail_url': detail_url
                            })
                    except: continue
        except Exception as e:
            logger.warning(f"Sulekha extraction error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None
    
    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class ClickIndiaScraper(BaseScraper):
    source_name = "CLICKINDIA"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = category.lower().replace(' ', '-')
        return f"https://www.clickindia.com/{cat}/{city.lower()}/?page={page}"
    
    async def get_detail_url(self, card) -> Optional[str]:
        try:
            link = await card.query_selector('h3 a, .listing-title a')
            if link:
                return await link.get_attribute('href')
        except:
            pass
        return None
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            if html_content:
                # Use BeautifulSoup for faster/cleaner parsing
                soup = BeautifulSoup(html_content, 'lxml')
                cards = soup.select('.listing-item, .result-item, .listing-card')
                for card in cards:
                    try:
                        name = card.select_one('h3, .title, .listing-title')
                        phone = card.select_one('.phone, .contact-no, span.contact')
                        addr = card.select_one('.address, .location, .city')
                        link = card.select_one('h3 a, .listing-title a, a.title')
                        
                        if name:
                            listings.append({
                                'name': name.get_text(strip=True),
                                'phone': self._clean_phone(phone.get_text(strip=True)) if phone else None,
                                'address': addr.get_text(strip=True) if addr else None,
                                'detail_url': link.get('href') if link else None
                            })
                    except:
                        continue
            else:
                # Fallback to Playwright
                await page.wait_for_selector('.listings, .search-results', timeout=10000)
                cards = await page.query_selector_all('.listing-item, .result-item')
                for card in cards:
                    try:
                        name = await self._get_text(card, 'h3, .title, .listing-title')
                        phone = await self._get_text(card, '.phone, .contact-no')
                        address = await self._get_text(card, '.address, .location')
                        detail_url = await self.get_detail_url(card)
                        if name:
                            listings.append({
                                'name': name.strip(),
                                'phone': self._clean_phone(phone),
                                'address': address.strip() if address else None,
                                'detail_url': detail_url
                            })
                    except: continue
        except Exception as e:
            logger.warning(f"ClickIndia extraction error: {e}")
        return listings
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if digits else None
    
    async def _get_text(self, card, selector: str) -> Optional[str]:
        elem = await card.query_selector(selector)
        return await elem.inner_text() if elem else None


class GrotalScraper(BaseScraper):
    source_name = "GROTAL"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        cat = category.replace(' ', '-')
        return f"https://www.grotal.com/{city.title()}/{cat}"
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            if html_content:
                soup = BeautifulSoup(html_content, 'lxml')
                cards = soup.select('.listing-item, div[id*="div_"], .result-box')
                for card in cards:
                    try:
                        name = card.select_one('h2, .title, .name')
                        phone = card.select_one('.contact, .mobile, .phone')
                        addr = card.select_one('.address, .location')
                        
                        if name and name.get_text(strip=True):
                            listings.append({
                                'name': name.get_text(strip=True),
                                'phone': self._clean_phone(phone.get_text(strip=True)) if phone else None,
                                'address': addr.get_text(strip=True) if addr else None,
                                'detail_url': None
                            })
                    except: continue
            else:
                cards = await page.query_selector_all('.listing-item, .result-box')
                for card in cards:
                    name = await card.query_selector('h2')
                    if name:
                        listings.append({
                            'name': await name.inner_text(),
                            'phone': None,
                            'address': None
                        })
        except Exception as e:
            logger.warning(f"Grotal extraction error: {e}")
        return listings

    def _clean_phone(self, phone: str) -> Optional[str]:
        if not phone: return None
        digits = re.sub(r'[^\d]', '', phone)
        return digits[-10:] if len(digits) >= 10 else digits

class ExportersIndiaScraper(BaseScraper):
    source_name = "EXPORTERSINDIA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        q = f"{category} in {city}".replace(" ", "+")
        return f"https://www.exportersindia.com/search.php?term={q}&page={page}"
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class AskLailaScraper(BaseScraper):
    source_name = "ASKLAILA"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.asklaila.com/search/{city.lower()}/{category.lower().replace(' ', '-')}/{(page-1)*20}/"
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)

class VykariScraper(BaseScraper):
    source_name = "VYKARI"
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return f"https://www.vykari.com/search/{city.lower()}/{category.lower().replace(' ', '-')}"
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        return self.extract_raw_fallback(html_content, city, category)



class SEBIScraper(BaseScraper):
    """Specialized scraper for SEBI Registered Investment Advisors and Intermediaries."""
    source_name = "SEBI"
    
    # Official portal for 2026
    SEARCH_URL = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.SEARCH_URL
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            # Phase 1: Interactive Search (Triggering the server-side action)
            # If we are in a live page, we might need to select the category
            try:
                if not html_content:
                    await page.wait_for_selector("select[name='intmId']", timeout=10000)
                    # 13 is the internal ID for Investment Advisers
                    await page.select_option("select[name='intmId']", "13")
                    await page.click("input[type='submit']")
                    await page.wait_for_load_state("networkidle")
            except: pass

            # Phase 2: High-Fidelity Extraction
            content = html_content or await page.content()
            soup = BeautifulSoup(content, 'lxml')
            
            # SEBI uses 'table-striped' and '#sample_1' for their data grids
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
                                'source': 'SEBI Registered'
                            })
            
            if not listings:
                logger.warning(f"SEBI: No leads in table. Checking for fallback regex.")
                listings = self.extract_raw_fallback(content, city, category)

        except Exception as e:
            logger.error(f"SEBI Enterprise Error: {e}")
            
        return listings


class NSEScraper(BaseScraper):
    """Specialized scraper for NSE Authorized Persons."""
    source_name = "NSE"
    
    SEARCH_URL = "https://www.nseindia.com/members/content/member_directory.htm"

    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        return self.SEARCH_URL
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            soup = BeautifulSoup(html_content or await page.content(), 'lxml')
            table = soup.select_one('table#memberDirectoryTable, .common_table')
            if table:
                rows = table.select('tr')[1:]
                for row in rows:
                    cols = row.select('td')
                    if len(cols) >= 4:
                        listings.append({
                            'name': cols[1].get_text(strip=True),
                            'phone': None,
                            'address': cols[3].get_text(strip=True),
                            'source_id': cols[0].get_text(strip=True)
                        })
        except Exception as e:
            logger.warning(f"NSE extraction error: {e}")
        return listings


class GoogleMapsScraper(BaseScraper):
    """Deep scraper for Google Maps Business (GMB) listings."""
    source_name = "GMB"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        query = f"{category} in {city}".replace(' ', '+')
        return f"https://www.google.com/maps/search/{query}"
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            # GMB is highly dynamic. We scroll to load results.
            # We only do this if we are in a live browser session (not just raw HTML)
            if not html_content:
                # Scroll result pane
                try:
                    await page.wait_for_selector('role=feed', timeout=10000)
                    for _ in range(3): # Scroll a few times to get more results
                        await page.mouse.wheel(0, 5000)
                        await asyncio.sleep(2)
                except: pass
                
            soup = BeautifulSoup(html_content or await page.content(), 'lxml')
            # GMB result cards often use specific aria-labels or roles
            cards = soup.select('div[role="article"], a[href*="/maps/place/"]')
            
            for card in cards:
                try:
                    # Look for name in aria-label or specific classes
                    name = card.get('aria-label') or card.select_one('.fontHeadlineSmall, .qBF1Pd')
                    if not name: continue
                    
                    name_text = name if isinstance(name, str) else name.get_text(strip=True)
                    if not name_text: continue

                    # Details are often in spans or specific classes
                    # Note: These selectors change often, we use a broad search
                    info_text = card.get_text(" | ", strip=True)
                    phone = self._extract_phone_from_text(info_text)
                    
                    listings.append({
                        'name': name_text,
                        'phone': phone,
                        'address': info_text.split('|')[1] if '|' in info_text else None, # Rough guess
                        'detail_url': card.get('href') if card.name == 'a' else None
                    })
                except: continue
        except Exception as e:
            logger.warning(f"GMB extraction error: {e}")
        return listings

    def _extract_phone_from_text(self, text: str) -> Optional[str]:
        # Simple regex for Indian phone numbers
        matches = re.findall(r'(?:\+91|0)?\s?[6789]\d{9}', text)
        return matches[0] if matches else None


class LinkedInGoogleScraper(BaseScraper):
    """Scrapes LinkedIn leads via Google Search to avoid account bans."""
    source_name = "LINKEDIN"
    
    def build_search_url(self, city: str, category: str, page: int = 1) -> str:
        # Use Google to find LinkedIn profiles
        query = f'site:linkedin.com/in/ "{category}" "{city}"'
        return f"https://www.google.com/search?q={query.replace(' ', '+')}&start={(page-1)*10}"
    
    async def extract_listings(self, page: Page, city: str = None, category: str = None, html_content: str = None) -> List[Dict]:
        listings = []
        try:
            soup = BeautifulSoup(html_content or await page.content(), 'lxml')
            # Google search result cards
            results = soup.select('.g, .tF2Cxc')
            
            for res in results:
                try:
                    title_elem = res.select_one('h3')
                    link_elem = res.select_one('a')
                    snippet_elem = res.select_one('.VwiC3b, .bAWN1e')
                    
                    if title_elem and link_elem:
                        full_title = title_elem.get_text(strip=True)
                        # LinkedIn titles in Google usually look like "John Doe - Senior Manager - Company | LinkedIn"
                        parts = full_title.split(' - ')
                        
                        listings.append({
                            'name': parts[0].strip(),
                            'phone': None, # LinkedIn profiles don't show phone on public search
                            'address': city,
                            'title': parts[1].strip() if len(parts) > 1 else None,
                            'detail_url': link_elem.get('href'),
                            'snippet': snippet_elem.get_text(strip=True) if snippet_elem else None
                        })
                except: continue
        except Exception as e:
            logger.warning(f"LinkedInGoogle extraction error: {e}")
        return listings


# ==================== Email Validation ====================

class EmailValidator:
    """Validate emails using multiple methods"""
    
    # Common disposable email domains to reject
    DISPOSABLE_DOMAINS = {
        'tempmail.com', '10minutemail.com', 'guerrillamail.com',
        'mailinator.com', 'throwaway.email', 'fakeinbox.com',
        'yopmail.com', 'trashmail.com', 'getnada.com'
    }
    
    VALID_DOMAINS = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
                     'rediffmail.com', 'icloud.com', 'protonmail.com', 'aol.com'}
    
    @staticmethod
    def extract_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        match = re.search(pattern, text)
        return match.group(0).lower() if match else None
    
    @staticmethod
    def is_valid_format(email: str) -> bool:
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def is_disposable(email: str) -> bool:
        if not email or '@' not in email:
            return True
        domain = email.split('@')[1].lower()
        return domain in EmailValidator.DISPOSABLE_DOMAINS
    
    @staticmethod
    def is_corporate(email: str) -> bool:
        """Check if email is corporate (not free)"""
        if not email or '@' not in email:
            return False
        domain = email.split('@')[1].lower()
        return domain not in EmailValidator.DISPOSABLE_DOMAINS
    
    @staticmethod
    async def verify_async(email: str) -> Dict:
        """Async email verification (basic)"""
        if not email or not EmailValidator.is_valid_format(email):
            return {'valid': False, 'reason': 'invalid_format'}
        
        if EmailValidator.is_disposable(email):
            return {'valid': False, 'reason': 'disposable'}
        
        domain = email.split('@')[1].lower()
        
        # Basic MX record check would go here
        # For now, return valid for corporate emails
        return {
            'valid': True,
            'domain': domain,
            'corporate': EmailValidator.is_corporate(email)
        }


# ==================== Phone Formatting ====================

class PhoneFormatter:
    """Standardize phone number formats"""
    
    @staticmethod
    def format_india(phone: str) -> str:
        """Format to +91-XXX-XXX-XXXX"""
        if not phone:
            return ""
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            last10 = digits[-10:]
            return f"+91-{last10[:3]}-{last10[3:6]}-{last10[6:]}"
        return phone
    
    @staticmethod
    def format_international(phone: str, country_code: str = "+91") -> str:
        """Format to international standard"""
        if not phone:
            return ""
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) >= 10:
            last10 = digits[-10:]
            return f"{country_code} {last10[:3]} {last10[3:6]} {last10[6:]}"
        return phone
    
    @staticmethod
    def clean_and_validate(phone: str) -> Optional[str]:
        """Clean and validate phone number"""
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) == 10:
            return digits
        elif len(digits) == 11 and digits[0] == '0':
            return digits[1:]
        elif len(digits) == 12 and digits[:2] == '91':
            return digits[2:]
        elif len(digits) == 13 and digits[:3] == '+91':
            return digits[3:]
        return digits if 8 <= len(digits) <= 15 else None


# ==================== Fuzzy Deduplication ====================

class FuzzyDeduplicator:
    """Fuzzy matching for deduplication"""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize name for comparison"""
        if not name:
            return ""
        name = name.lower().strip()
        name = re.sub(r'[^a-z\s]', '', name)
        name = ' '.join(name.split())
        return name
    
    @staticmethod
    def similarity(s1: str, s2: str) -> float:
        """Calculate similarity between two strings (0-1)"""
        if not s1 or not s2:
            return 0.0
        s1, s2 = s1.lower(), s2.lower()
        if s1 == s2:
            return 1.0
        if s1 in s2 or s2 in s1:
            return 0.8
        # Simple character-based similarity
        common = sum(1 for c in s1 if c in s2)
        return common / max(len(s1), len(s2))
    
    @staticmethod
    def is_duplicate(contact1: Dict, contact2: Dict, threshold: float = 0.85) -> bool:
        """Check if two contacts are duplicates"""
        # Phone match (exact)
        if contact1.get('phone_clean') and contact2.get('phone_clean'):
            if contact1['phone_clean'] == contact2['phone_clean']:
                return True
        
        # Phone match (last 10 digits)
        p1 = contact1.get('phone_clean', '') or contact1.get('phone', '') or ''
        p2 = contact2.get('phone_clean', '') or contact2.get('phone', '') or ''
        p1 = re.sub(r'[^\d]', '', p1)[-10:] if p1 else ''
        p2 = re.sub(r'[^\d]', '', p2)[-10:] if p2 else ''
        if p1 and p2 and p1 == p2:
            return True
        
        # Email match
        e1 = (contact1.get('email') or '').lower()
        e2 = (contact2.get('email') or '').lower()
        if e1 and e2 and e1 == e2:
            return True
        
        # Name + City fuzzy match
        n1 = FuzzyDeduplicator.normalize_name(contact1.get('name', ''))
        n2 = FuzzyDeduplicator.normalize_name(contact2.get('name', ''))
        c1 = (contact1.get('city') or '').lower()
        c2 = (contact2.get('city') or '').lower()
        
        if n1 and n2 and c1 and c2:
            name_sim = FuzzyDeduplicator.similarity(n1, n2)
            if name_sim >= threshold and c1 == c2:
                return True
        
        return False
    
    @staticmethod
    def find_duplicates(contacts: List[Dict]) -> List[List[Dict]]:
        """Find all duplicate groups in a list of contacts"""
        duplicates = []
        processed = set()
        
        for i, contact in enumerate(contacts):
            if i in processed:
                continue
            
            group = [contact]
            for j in range(i + 1, len(contacts)):
                if j in processed:
                    continue
                if FuzzyDeduplicator.is_duplicate(contact, contacts[j]):
                    group.append(contacts[j])
                    processed.add(j)
            
            if len(group) > 1:
                duplicates.append(group)
                processed.add(i)
        
        return duplicates


# ==================== Quality Scoring ====================

class QualityScorer:
    """Score contacts based on data completeness"""
    
    WEIGHTS = {
        'phone': 25,
        'email': 25,
        'address': 20,
        'city': 15,
        'area': 10,
        'source': 5
    }
    
    @staticmethod
    def score(contact: Dict) -> int:
        """Calculate quality score (0-100)"""
        score = 0
        
        # Phone scoring
        phone = contact.get('phone') or contact.get('phone_clean')
        if phone:
            digits = re.sub(r'[^\d]', '', str(phone))
            if len(digits) >= 10:
                score += QualityScorer.WEIGHTS['phone']
        
        # Email scoring
        email = contact.get('email')
        if email and EmailValidator.is_valid_format(email):
            if not EmailValidator.is_disposable(email):
                score += QualityScorer.WEIGHTS['email']
        
        # Address scoring
        if contact.get('address'):
            score += QualityScorer.WEIGHTS['address']
        
        # City scoring
        if contact.get('city'):
            score += QualityScorer.WEIGHTS['city']
        
        # Area scoring
        if contact.get('area'):
            score += QualityScorer.WEIGHTS['area']
        
        # Has source
        if contact.get('source'):
            score += QualityScorer.WEIGHTS['source']
        
        return min(score, 100)
    
    @staticmethod
    def get_tier(score: int) -> str:
        """Get quality tier"""
        if score >= 70:
            return 'high'
        elif score >= 40:
            return 'medium'
        return 'low'
    
    @staticmethod
    def score_batch(contacts: List[Dict]) -> List[Dict]:
        """Score a batch of contacts"""
        scored = []
        for contact in contacts:
            contact['quality_score'] = QualityScorer.score(contact)
            contact['quality_tier'] = QualityScorer.get_tier(contact['quality_score'])
            scored.append(contact)
        return scored


# ==================== Error Recovery ====================

class RetryQueue:
    """Queue for retrying failed scrape operations"""
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.queue: List[Dict] = []
        self.failed_urls = {}
    
    def add(self, url: str, category: str, city: str, source: str, error: str = None):
        """Add a URL to retry queue"""
        key = f"{source}_{category}_{city}_{url}"
        
        if key not in self.failed_urls:
            self.failed_urls[key] = {
                'url': url,
                'category': category,
                'city': city,
                'source': source,
                'retries': 0,
                'error': error,
                'added_at': datetime.now()
            }
    
    def should_retry(self, key: str) -> bool:
        """Check if URL should be retried"""
        if key not in self.failed_urls:
            return False
        return self.failed_urls[key]['retries'] < self.max_retries
    
    def increment_retry(self, key: str):
        """Increment retry count"""
        if key in self.failed_urls:
            self.failed_urls[key]['retries'] += 1
    
    def get_pending(self) -> List[Dict]:
        """Get all URLs pending retry"""
        return [v for k, v in self.failed_urls.items() if self.should_retry(k)]
    
    def save_to_file(self, filepath: str):
        """Save queue to file for persistence"""
        import json
        with open(filepath, 'w') as f:
            json.dump(self.failed_urls, f, indent=2, default=str)
    
    def load_from_file(self, filepath: str):
        """Load queue from file"""
        import json
        try:
            with open(filepath, 'r') as f:
                self.failed_urls = json.load(f)
        except:
            pass


# ==================== Scheduler ====================

class ScraperScheduler:
    """Simple scheduler for periodic scraping"""
    
    def __init__(self, interval_hours: int = 24):
        self.interval_hours = interval_hours
        self.last_run = None
    
    def should_run(self) -> bool:
        """Check if scraper should run"""
        if not self.last_run:
            return True
        elapsed = datetime.now() - self.last_run
        return elapsed.total_seconds() >= (self.interval_hours * 3600)
    
    def record_run(self):
        """Record that scraper ran"""
        self.last_run = datetime.now()
    
    async def wait_until_next_run(self):
        """Wait until next scheduled run"""
        if not self.should_run():
            elapsed = datetime.now() - self.last_run
            wait_seconds = (self.interval_hours * 3600) - elapsed.total_seconds()
            if wait_seconds > 0:
                logger.info(f"Waiting {wait_seconds/3600:.1f} hours until next run")
                await asyncio.sleep(wait_seconds)


# ==================== Site Selector Updates ====================

class SelectorManager:
    """Manage and update CSS selectors for different sites"""
    
    SELECTORS = {
        'justdial': {
            'container': '.store-list, .jl-contacts',
            'card': '.store-info, .jl-row',
            'name': '.store-name, .contact-name, h2 a',
            'phone': '.store-phone, .contact-mobile, [class*="phone"]',
            'address': '.store-address, .address, .contact-address',
            'area': '.store-area, .area, .locality'
        },
        'indiamart': {
            'container': '.prod-list, .search-result',
            'card': '.prod-item, .result-item',
            'name': '.prod-name, .product-title',
            'phone': '.prod-phn, .contact-num',
            'address': '.prod-addr, .address'
        },
        'sulekha': {
            'container': '.business-list, .results',
            'card': '.business-card, .result-card',
            'name': '.business-name, .title',
            'phone': '.phone, .contact',
            'address': '.address, .location'
        },
        'clickindia': {
            'container': '.listings, .results',
            'card': '.listing-item, .result',
            'name': '.title, h3',
            'phone': '.phone, .contact',
            'address': '.address, .location'
        }
    }
    
    @classmethod
    def get_selectors(cls, site: str) -> Dict:
        """Get selectors for a site"""
        return cls.SELECTORS.get(site.lower(), {})
    
    @classmethod
    def update_selector(cls, site: str, element: str, new_selector: str):
        """Update a specific selector"""
        if site.lower() not in cls.SELECTORS:
            cls.SELECTORS[site.lower()] = {}
        cls.SELECTORS[site.lower()][element] = new_selector
    
    @classmethod
    def save_to_config(cls, filepath: str):
        """Save selectors to config file"""
        import yaml
        with open(filepath, 'w') as f:
            yaml.dump({'selectors': cls.SELECTORS}, f)
    
    @classmethod
    def load_from_config(cls, filepath: str):
        """Load selectors from config file"""
        import yaml
        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
                if data and 'selectors' in data:
                    cls.SELECTORS.update(data['selectors'])
        except:
            pass
