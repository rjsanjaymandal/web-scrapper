"""
Enhanced Scraper Utilities Module
Additional scrapers, validation, and utilities for more efficient data collection
"""

import asyncio
import re
import aiohttp
import hashlib
import logging
from typing import Optional, Dict, List
from datetime import datetime
from playwright.async_api import Page
from scrapers_registry import BaseScraper, ScraperRegistry

logger = logging.getLogger(__name__)

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
    
    async def extract_listings(self, page: Page) -> List[Dict]:
        listings = []
        try:
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
                except:
                    continue
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
    
    async def extract_listings(self, page: Page) -> List[Dict]:
        listings = []
        try:
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
                except:
                    continue
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
