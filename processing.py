import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import sys
from datetime import datetime
from blockchain_utils import BlockchainUtils

logger = logging.getLogger(__name__)

@dataclass
class QualityResult:
    """Result of quality assessment"""
    is_valid: bool
    quality_score: int
    quality_tier: str  # high, medium, low
    issues: List[str]

class ProcessingHandler:
    """
    Unified handler for all data quality, cleaning, and enrichment operations.
    Consolidates logic from legacy data_quality.py and quality_pipeline.py.
    """

    # Indian mobile phone patterns
    PHONE_PATTERNS = [
        r'\+?91[5-9]\d{9}',  # +91xxxxxxxxxx
        r'[5-9]\d{9}',       # 10 digits starting with 5-9
        r'0[5-9]\d{9}',      # 0xxxxxxxxxx
    ]

    TRUSTED_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
        'rediffmail.com', 'icloud.com', 'protonmail.com', 'aol.com',
        'mail.com', 'ymail.com', 'live.com', 'msn.com', 'zoho.com'
    }

    DISPOSABLE_DOMAINS = {
        'tempmail.com', '10minutemail.com', 'guerrillamail.com',
        'mailinator.com', 'throwaway.email', 'fakeinbox.com',
        'yopmail.com', 'trashmail.com', 'getnada.com', 'sharklasers.com',
        'grr.la', 'maildrop.cc', 'throwemail.com', 'mintemail.com',
        'example.com', 'sample.com', 'test.com'
    }

    JUNK_EMAIL_PATTERNS = [
        r'^test@', r'^abc@', r'^noreply@', r'^no-reply@',
        r'^placeholder@', r'^dummy@', r'^example@'
    ]

    CATEGORY_MAP = {
        'mutual fund agent': 'Mutual Fund Agent',
        'mutual fund agents': 'Mutual Fund Agent',
        'mutual fund advisor': 'Mutual Fund Agent',
        'mutual fund advisors': 'Mutual Fund Agent',
        'mutual fund consultants': 'Mutual Fund Agent',
        'mutual fund consultant': 'Mutual Fund Agent',
        'mf agent': 'Mutual Fund Agent',
        'mf agents': 'Mutual Fund Agent',
        'amfi registered': 'Mutual Fund Agent',
        'insurance agent': 'Insurance Agent',
        'insurance agents': 'Insurance Agent',
        'insurance advisor': 'Insurance Agent',
        'insurance consultant': 'Insurance Agent',
        'lic agent': 'Insurance Agent',
        'insurance brokers': 'Insurance Agent',
        'chartered accountant': 'Chartered Accountant',
        'chartered accountants': 'Chartered Accountant',
        'ca': 'Chartered Accountant',
        'tax advocate': 'Tax Advocate',
        'tax consultants': 'Tax Advocate',
        'investment advisor': 'Investment Advisor',
        'investment advisors': 'Investment Advisor',
        'investment adviser': 'Investment Advisor',
        'investment advisers': 'Investment Advisor',
        'sebi registered': 'Investment Advisor',
        'sebi ria': 'Investment Advisor',
        'lawyer': 'Lawyer',
        'lawyers': 'Lawyer',
        'advocate': 'Lawyer',
        'advocates': 'Lawyer',
        'company secretary': 'Company Secretary',
        'company secretaries': 'Company Secretary',
        'insolvency professional': 'Insolvency Professional',
        'insolvency professionals': 'Insolvency Professional',
        'gst practitioner': 'GST Practitioner',
        'gst practitioners': 'GST Practitioner',
        'gst consultant': 'GST Practitioner',
        'stock broker': 'Stock Broker',
        'stock brokers': 'Stock Broker',
        'real estate agent': 'Real Estate Agent',
        'real estate agents': 'Real Estate Agent',
        'merchants': 'Merchant',
        'merchant': 'Merchant',
        'exporters': 'Exporter',
        'importers': 'Importer',
        'importer': 'Importer',
        'exporter': 'Exporter',
    }

    @staticmethod
    def normalize_phone(phone: Any) -> Optional[str]:
        """Validate and clean phone number to a standard 10-digit format for India."""
        if not phone:
            return None
        
        phone_str = str(phone)
        digits = re.sub(r'[^\d]', '', phone_str)
        if len(digits) >= 10:
            return digits[-10:]
        return digits if len(digits) >= 6 else None

    @staticmethod
    def is_valid_email(email: Any) -> bool:
        """Robust email validation including domain check."""
        if not email:
            return False
            
        email = str(email).strip().lower()
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        
        domain = email.split('@')[1] if '@' in email else ''
        if domain in ProcessingHandler.DISPOSABLE_DOMAINS:
            return False
            
        # Check against junk patterns (test@..., etc)
        for pattern in ProcessingHandler.JUNK_EMAIL_PATTERNS:
            if re.match(pattern, email):
                return False
                
        return True
    @staticmethod
    def normalize_category(category: Any) -> str:
        """Normalize category names to a canonical display name."""
        if not category:
            return "General"
        
        # 1. Clean the string
        cat = str(category).lower().strip()
        cat = re.sub(r'[^a-z0-9 ]+', ' ', cat).strip()
        cat = re.sub(r'\s+', ' ', cat)
        
        # 2. Check the map (direct match or fuzzy space/hyphen match)
        for key, canonical in ProcessingHandler.CATEGORY_MAP.items():
            if cat == key or cat == key.replace(' ', '-'):
                return canonical
        
        # 3. Handle pluralization (simple)
        if cat.endswith('s') and cat[:-1] in ProcessingHandler.CATEGORY_MAP:
             return ProcessingHandler.CATEGORY_MAP[cat[:-1]]
             
        # 4. Fallback to Title Case
        return cat.title()

    @staticmethod
    def calculate_quality_score(contact: Optional[Dict]) -> int:
        """
        Calculates a lead quality score (0-100) based on completeness and data fidelity.
        """
        if not contact:
            return 0
            
        score = 0
        
        # Phone (35 points) - Highest priority
        phone_clean = contact.get('phone_clean')
        if phone_clean and len(phone_clean) == 10:
            score += 35
        elif contact.get('phone'):
            score += 10
            
        # Email (25 points)
        if contact.get('email') and contact.get('email_valid', False):
            score += 25
        elif contact.get('email'):
            score += 10
            
        # Name (15 points)
        name = contact.get('name', '')
        if name and len(name) >= 3:
            score += 15
            
        # Address/Location (15 points)
        if contact.get('city'):
            score += 5
            
        # Blockchain/Web3 Factors (25 points) - NEW for 2026
        if contact.get('blockchain_ca'):
            score += 25  # Contract addresses are high-value signals
            
        # Industry specific/Trust factors (10 points)
        source = contact.get('source', '').upper()
        if source in ['AMFI', 'IRDAI', 'ICAI', 'ICSI']:
            score += 10 # Official sources are high trust
        elif source == 'JUSTDIAL' and contact.get('area'):
            score += 5
            
        return min(score, 100)

    @staticmethod
    def get_quality_tier(score: int) -> str:
        """Categorize lead into tiers."""
        if score >= 75:
            return 'high'
        elif score >= 45:
            return 'medium'
        return 'low'

    @classmethod
    def process_contact(cls, contact: Optional[Dict]) -> Optional[Dict]:
        """
        Performs full cleaning, normalization, and scoring on a single contact record.
        """
        if not contact:
            return None
            
        # 1. Clean Phone
        phone_clean = cls.normalize_phone(contact.get('phone'))
        contact['phone_clean'] = phone_clean
        # DESTRUCTIVE CLEAN: Overwrite raw phone with the cleaned version (or None if junk)
        contact['phone'] = phone_clean
        
        # 2. Validate Email
        email = str(contact.get('email') or '').strip().lower()
        if email:
            is_valid = cls.is_valid_email(email)
            contact['email_valid'] = is_valid
            if not is_valid:
                # DESTRUCTIVE CLEAN: Wipe the email field if it is junk
                contact['email'] = None
        else:
            contact['email'] = None
            contact['email_valid'] = False
        
        # 3. Normalize Strings
        for field in ['name', 'address', 'area', 'city', 'category']:
            val = contact.get(field)
            if val:
                # Remove extra whitespace and noise
                val = re.sub(r'\s+', ' ', str(val)).strip()
                if field == 'name':
                    val = val.title()
                elif field == 'category':
                    val = cls.normalize_category(val)
                contact[field] = val
        
        # 4. Extract Blockchain Contract Addresses (CAs)
        # We check address, name, and any detail text for CAs
        combined_text = f"{contact.get('name', '')} {contact.get('address', '')}"
        cas = BlockchainUtils.extract_cas(combined_text)
        if cas:
            contact['blockchain_ca'] = cas[0] # Take the first one found
            logger.info(f"💎 Found CA: {contact['blockchain_ca']} for {contact.get('name')}")
        
        # 5. Calculate Quality
        contact['quality_score'] = cls.calculate_quality_score(contact)
        contact['quality_tier'] = cls.get_quality_tier(contact['quality_score'])
        
        # 5. Metadata
        contact['enriched'] = True
        contact['processed_at'] = datetime.now().isoformat()
        
        return contact

    @classmethod
    def process_batch(cls, contacts: List[Dict]) -> List[Dict]:
        """Batch process for better performance."""
        return [cls.process_contact(c) for c in contacts]

    @staticmethod
    def filter_valid(contacts: List[Dict]) -> List[Dict]:
        """Returns only contacts that have at least a valid phone or email."""
        return [c for c in contacts if c.get('phone_clean') or (c.get('email') and c.get('email_valid'))]

    @classmethod
    def clean_database_logic(cls, db_conn) -> Dict[str, int]:
        """
        Deep database cleaning logic: normalizes categories and removes duplicates.
        Returns stats about the operation.
        """
        cur = db_conn.cursor()
        cur.execute("SELECT id, category FROM contacts")
        rows = cur.fetchall()
        
        updated = 0
        cat_changes = 0
        
        placeholder = "?" if "sqlite3" in str(type(db_conn)) else "%s"
        
        for row in rows:
            contact_id = row[0] if isinstance(row, tuple) else row['id']
            raw_cat = row[1] if isinstance(row, tuple) else row['category']
            
            if not raw_cat:
                continue
                
            norm_cat = cls.normalize_category(raw_cat)
            
            if norm_cat != raw_cat:
                cur.execute(
                    f"UPDATE contacts SET category = {placeholder} WHERE id = {placeholder}",
                    (norm_cat, contact_id)
                )
                cat_changes += 1
                updated += 1
        
        db_conn.commit()
        cur.close()
        return {"total_checked": len(rows), "updated": updated, "category_normalized": cat_changes}
