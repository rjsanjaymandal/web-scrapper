import re
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataQualityPipeline:
    """Standardizes data quality and leads scoring."""

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
    def is_valid_email(email: str) -> bool:
        if not email: return False
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email.strip()))

    @staticmethod
    def calculate_score(contact: Dict) -> int:
        """Score a contact from 0-100 based on completeness."""
        score = 0
        if contact.get('name') and len(contact['name']) > 2: score += 20
        if contact.get('phone_clean'): score += 30
        if contact.get('email_valid'): score += 20
        if contact.get('address'): score += 15
        if contact.get('city'): score += 10
        if contact.get('source') in ['AMFI', 'IRDAI', 'ICAI']: score += 5
        return min(score, 100)

    @staticmethod
    def get_tier(score: int) -> str:
        if score >= 70: return 'high'
        if score >= 40: return 'medium'
        return 'low'

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
