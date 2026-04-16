import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime

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

    @staticmethod
    def normalize_phone(phone: Any) -> Optional[str]:
        """Validate and clean phone number to a standard 10-digit format for India."""
        if not phone:
            return None
        
        digits = re.sub(r'[^\d]', '', str(phone))
        
        # Handle different Indian formats
        if len(digits) == 10:
            clean = digits
        elif len(digits) == 11 and digits.startswith('0'):
            clean = digits[1:]
        elif len(digits) == 12 and digits.startswith('91'):
            clean = digits[2:]
        elif len(digits) > 10:
            clean = digits[-10:]
        else:
            return None

        # Validate Indian mobile (first digit should be 5-9)
        if clean[0] not in '56789':
            return None
            
        return clean

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
    def calculate_quality_score(contact: Dict) -> int:
        """
        Calculates a lead quality score (0-100) based on completeness and data fidelity.
        """
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
        if contact.get('address'):
            score += 10
        if contact.get('city'):
            score += 5
            
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
    def process_contact(cls, contact: Dict) -> Dict:
        """
        Performs full cleaning, normalization, and scoring on a single contact record.
        """
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
                contact[field] = val
        
        # 4. Calculate Quality
        contact['quality_score'] = cls.calculate_score(contact) if hasattr(cls, 'calculate_score') else cls.calculate_quality_score(contact)
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
