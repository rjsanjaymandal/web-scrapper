"""
Data Quality Module
Comprehensive data validation, cleaning, and quality scoring
"""

import re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass


@dataclass
class QualityResult:
    """Result of quality assessment"""
    is_valid: bool
    quality_score: int
    quality_tier: str  # high, medium, low
    issues: List[str]


class DataQualityHandler:
    """Handle all data quality operations"""
    
    # Indian mobile phone patterns
    PHONE_PATTERNS = [
        r'\+?91[5-9]\d{9}',  # +91xxxxxxxxxx
        r'[5-9]\d{9}',       # 10 digits starting with 5-9
        r'0[5-9]\d{9}',      # 0xxxxxxxxxx
    ]
    
    # Valid email domains (not disposable)
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
    def validate_phone(phone: str) -> Tuple[Optional[str], bool, List[str]]:
        """
        Validate and clean phone number
        Returns: (clean_phone, is_valid, issues)
        """
        issues = []
        if not phone:
            return None, False, ["Phone is empty"]
        
        # Extract digits
        digits = re.sub(r'[^\d]', '', str(phone))
        
        if not digits:
            return None, False, ["No digits in phone number"]
        
        # Extract last 10 digits (Indian mobile)
        if len(digits) >= 10:
            clean = digits[-10:]
        elif len(digits) >= 8:
            clean = digits
            issues.append("Phone number less than 10 digits")
        else:
            return None, False, ["Phone number too short"]
        
        # Validate Indian mobile (first digit should be 5-9)
        if clean[0] not in '56789':
            return None, False, ["Invalid Indian mobile number"]
        
        return clean, True, issues
    
    @staticmethod
    def validate_email(email: str) -> Tuple[Optional[str], bool, List[str]]:
        """
        Validate email address
        Returns: (clean_email, is_valid, issues)
        """
        issues = []
        if not email:
            return None, False, ["Email is empty"]
        
        # Basic format validation
        email = str(email).strip().lower()
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if not re.match(pattern, email):
            return None, False, ["Invalid email format"]
        
        # Check domain
        if '@' not in email:
            return None, False, ["Invalid email format"]
        
        domain = email.split('@')[1]
        
        if domain in DataQualityHandler.DISPOSABLE_DOMAINS:
            return None, False, ["Disposable email domain"]
        
        return email, True, issues
    
    @staticmethod
    def calculate_quality_score(contact: Dict) -> int:
        """
        Calculate quality score (0-100)
        Based on completeness and validity of data
        """
        score = 0
        
        # Phone (30 points)
        phone_clean = contact.get('phone_clean') or ''
        if phone_clean and len(phone_clean) == 10:
            score += 30
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
        
        # Address (15 points)
        if contact.get('address'):
            score += 15
        
        # City (5 points)
        if contact.get('city'):
            score += 5
        
        # Source/Category (5 points)
        if contact.get('source') and contact.get('category'):
            score += 5
        
        # ARN/License (5 points)
        arn = contact.get('arn') or contact.get('license_no') or contact.get('membership_no')
        if arn:
            score += 5
        
        return min(score, 100)
    
    @staticmethod
    def get_quality_tier(score: int) -> str:
        """Get quality tier based on score"""
        if score >= 80:
            return 'high'
        elif score >= 50:
            return 'medium'
        return 'low'
    
    @staticmethod
    def assess_quality(contact: Dict) -> QualityResult:
        """Complete quality assessment of a contact"""
        issues = []
        
        # Validate phone
        phone_clean, phone_valid, phone_issues = DataQualityHandler.validate_phone(
            contact.get('phone', '')
        )
        issues.extend(phone_issues)
        
        # Validate email
        email_clean, email_valid, email_issues = DataQualityHandler.validate_email(
            contact.get('email', '')
        )
        issues.extend(email_issues)
        
        # Calculate score
        contact['phone_clean'] = phone_clean
        contact['email_valid'] = email_valid
        score = DataQualityHandler.calculate_quality_score(contact)
        tier = DataQualityHandler.get_quality_tier(score)
        
        is_valid = phone_valid or email_valid
        
        return QualityResult(
            is_valid=is_valid,
            quality_score=score,
            quality_tier=tier,
            issues=issues
        )
    
    @staticmethod
    def process_contact(contact: Dict) -> Dict:
        """
        Process a contact through quality pipeline
        Returns cleaned contact with quality fields
        """
        # Validate and clean phone
        phone_clean, phone_valid, _ = DataQualityHandler.validate_phone(
            contact.get('phone', '')
        )
        contact['phone_clean'] = phone_clean
        contact['phone_valid'] = phone_valid
        
        # Validate email
        email_clean, email_valid, _ = DataQualityHandler.validate_email(
            contact.get('email', '')
        )
        contact['email'] = email_clean
        contact['email_valid'] = email_valid
        
        # Clean name
        if contact.get('name'):
            contact['name'] = DataQualityHandler.clean_name(contact['name'])
        
        # Clean address
        if contact.get('address'):
            contact['address'] = DataQualityHandler.clean_address(contact['address'])
        
        # Calculate quality
        contact['quality_score'] = DataQualityHandler.calculate_quality_score(contact)
        contact['quality_tier'] = DataQualityHandler.get_quality_tier(contact['quality_score'])
        
        return contact
    
    @staticmethod
    def clean_name(name: str) -> str:
        """Clean and normalize name"""
        if not name:
            return ''
        name = str(name).strip()
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'[^\w\s\.\-]', '', name)
        return name.title()
    
    @staticmethod
    def clean_address(address: str) -> str:
        """Clean and normalize address"""
        if not address:
            return ''
        address = str(address).strip()
        address = re.sub(r'\s+', ' ', address)
        return address
    
    @staticmethod
    def process_batch(contacts: List[Dict]) -> List[Dict]:
        """Process a batch of contacts"""
        processed = []
        for contact in contacts:
            processed.append(DataQualityHandler.process_contact(contact))
        return processed
    
    @staticmethod
    def filter_valid_contacts(contacts: List[Dict]) -> List[Dict]:
        """Filter contacts that have at least phone or email"""
        valid = []
        for contact in contacts:
            if contact.get('phone_clean') or contact.get('email'):
                valid.append(contact)
        return valid
    
    @staticmethod
    def get_quality_stats(contacts: List[Dict]) -> Dict:
        """Get quality statistics for a list of contacts"""
        if not contacts:
            return {'total': 0, 'high': 0, 'medium': 0, 'low': 0, 'avg_score': 0}
        
        stats = {
            'total': len(contacts),
            'high': 0,
            'medium': 0,
            'low': 0,
            'with_phone': 0,
            'with_email': 0,
            'with_both': 0,
            'avg_score': 0
        }
        
        total_score = 0
        for c in contacts:
            tier = c.get('quality_tier', 'low')
            stats[tier] = stats.get(tier, 0) + 1
            
            if c.get('phone_clean'):
                stats['with_phone'] += 1
            if c.get('email'):
                stats['with_email'] += 1
            if c.get('phone_clean') and c.get('email'):
                stats['with_both'] += 1
            
            total_score += c.get('quality_score', 0)
        
        stats['avg_score'] = round(total_score / len(contacts), 1)
        return stats
    
    @staticmethod
    def remove_duplicates(contacts: List[Dict], by: str = 'phone') -> List[Dict]:
        """
        Remove duplicates by specified field
        by: 'phone', 'email', or 'both'
        Keeps the first occurrence
        """
        seen = set()
        unique = []
        
        for contact in contacts:
            key = None
            
            if by == 'phone':
                key = contact.get('phone_clean')
            elif by == 'email':
                key = contact.get('email', '').lower() if contact.get('email') else None
            elif by == 'both':
                phone = contact.get('phone_clean')
                email = contact.get('email', '').lower() if contact.get('email') else ''
                key = f"{phone}:{email}"
            
            if key and key not in seen:
                seen.add(key)
                unique.append(contact)
            elif not key:
                unique.append(contact)
        
        return unique
    
    @staticmethod
    def get_duplicate_count(contacts: List[Dict], by: str = 'phone') -> int:
        """Count duplicates in contacts"""
        seen = set()
        duplicates = 0
        
        for contact in contacts:
            key = None
            
            if by == 'phone':
                key = contact.get('phone_clean')
            elif by == 'email':
                key = contact.get('email', '').lower() if contact.get('email') else None
            elif by == 'both':
                phone = contact.get('phone_clean')
                email = contact.get('email', '').lower() if contact.get('email') else ''
                key = f"{phone}:{email}"
            
            if key:
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
        
        return duplicates
