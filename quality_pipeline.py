import re
import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
try:
    import dns.resolver
    HAS_DNS = True
except ImportError:
    HAS_DNS = False

logger = logging.getLogger(__name__)

class DataQualityPipeline:
    """Standardizes data quality and leads scoring."""

    TRUSTED_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
        'rediffmail.com', 'icloud.com', 'protonmail.com', 'aol.com',
        'mail.com', 'ymail.com', 'live.com', 'msn.com'
    }
    
    DISPOSABLE_DOMAINS = {
        'tempmail.com', '10minutemail.com', 'guerrillamail.com',
        'mailinator.com', 'throwaway.email', 'fakeinbox.com',
        'yopmail.com', 'trashmail.com', 'getnada.com', 'sharklasers.com',
        'grr.la', 'maildrop.cc', 'throwemail.com', 'mintemail.com'
    }

    @staticmethod
    def normalize_phone(phone: str) -> Optional[str]:
        if not phone: return None
        digits = re.sub(r'[^\d]', '', str(phone))
        if len(digits) == 10:
            return digits
        elif len(digits) == 11 and digits.startswith('0'):
            return digits[1:]
        elif len(digits) == 12 and digits.startswith('91'):
            return digits[2:]
        elif len(digits) > 10:
            return digits[-10:]
        return digits if 8 <= len(digits) <= 15 else None

    @staticmethod
    def check_mx_record(email: str) -> bool:
        """Deep check if email domain has a valid MX record."""
        if not HAS_DNS or not email or '@' not in email:
            return True # Fallback to true if no DNS or invalid email
        
        domain = email.split('@')[1]
        try:
            # We use a 2s timeout to avoid blocking too long
            # This is "Best in Class" verification
            dns.resolver.resolve(domain, 'MX', tcp=False, lifetime=2.0)
            return True
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout, Exception):
            return False

    @staticmethod
    def is_valid_email(email: str) -> bool:
        if not email: return False
        email = email.strip().lower()
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        
        domain = email.split('@')[1] if '@' in email else ''
        if domain in DataQualityPipeline.DISPOSABLE_DOMAINS:
            return False
        
        # Deep MX Check
        if not DataQualityPipeline.check_mx_record(email):
            return False
            
        return True

    @staticmethod
    def calculate_score(contact: Dict) -> int:
        """Premium score (0-100) based on deep verification."""
        score = 0
        
        # 1. Phone (35 points) - Highest importance
        if contact.get('phone_clean'):
            score += 35
        elif contact.get('phone'):
            score += 10
            
        # 2. Email (25 points)
        if contact.get('email_valid'):
            score += 25
            # Bonus for corporate email
            domain = contact.get('email', '').split('@')[-1]
            if domain not in DataQualityPipeline.TRUSTED_DOMAINS and domain:
                score += 5 # "Best in Class" prefers corporate leads
        elif contact.get('email'):
            score += 5
            
        # 3. Name & Identity (20 points)
        name = contact.get('name', '')
        if name and len(name) > 3:
            score += 15
        if contact.get('arn') or contact.get('license_no'):
            score += 5 # Official cross-verification bonus
            
        # 4. Location (15 points)
        if contact.get('address'): score += 10
        if contact.get('city'): score += 5
        
        return min(score, 100)

    @staticmethod
    def get_tier(score: int) -> str:
        if score >= 70: return 'high'
        if score >= 40: return 'medium'
        return 'low'

    @classmethod
    def merge_contacts(cls, existing: Dict, new: Dict) -> Dict:
        """
        Synthesize two versions of the same lead into one 'Golden Record'.
        Prioritizes non-empty values and official sources.
        """
        combined = dict(existing)
        
        # Define priority fields (can be expanded)
        fields = ['name', 'phone', 'email', 'address', 'city', 'area', 'state', 'arn', 'license_no', 'membership_no']
        
        for f in fields:
            val_existing = existing.get(f)
            val_new = new.get(f)
            
            # Simple merge: prefer the new one if the existing one is empty
            if not val_existing and val_new:
                combined[f] = val_new
            # Advanced merge: prefer official source if available
            elif val_new and new.get('source') in ['AMFI', 'IRDAI', 'ICAI']:
                combined[f] = val_new
                
        # Re-verify and re-score the merged result
        return cls.enrich_contact(combined)

    @classmethod
    def enrich_contact(cls, contact: Dict) -> Dict:
        """Full enrichment and validation of a single contact."""
        contact['phone_clean'] = cls.normalize_phone(contact.get('phone'))
        email = contact.get('email', '').strip().lower()
        contact['email'] = email
        contact['email_valid'] = cls.is_valid_email(email)
        
        score = cls.calculate_score(contact)
        contact['quality_score'] = score
        contact['quality_tier'] = cls.get_tier(score)
        contact['enriched'] = True
        return contact

    @classmethod
    def enrich_batch(cls, contacts: List[Dict]) -> List[Dict]:
        return [cls.enrich_contact(c) for c in contacts]
