import random
import logging
import asyncio
import re
from typing import Dict, List, Optional
# Removed Playwright dependencies to save memory and avoid WAFs
try:
    from fake_useragent import UserAgent
    ua_generator = UserAgent()
except ImportError:
    ua_generator = None

import uuid

logger = logging.getLogger(__name__)

class DataImpulseManager:
    """Specialized manager for DataImpulse Enterprise Proxy parameters."""
    
    @staticmethod
    def format_auth(
        username: str, 
        city: Optional[str] = None, 
        session_id: Optional[str] = None, 
        duration: int = 300,
        enable_city: bool = False 
    ) -> str:
        """
        Formats username with sticky sessions and location parameters.
        Format: user__sid.{sid};cr.{country};ct.{city};intvlv.{seconds}
        """
        if not username:
            return ""
            
        # If it's not a DataImpulse user, return as is
        # (DataImpulse usernames are usually long hex strings or contain 'dataimpulse')
        if len(username) < 10 and "data" not in username.lower():
            return username

        params = []
        
        # 1. Location Targeting
        if enable_city and city:
            # City targeting (Costs 2x)
            # Normalize city name: lowercase, no spaces
            clean_city = city.lower().replace(" ", "")
            params.append(f"ct.{clean_city}")
            # Country is implied but we can be explicit
            params.append("cr.in")
        else:
            # Standard Country targeting (Default: India)
            if "__cr." not in username:
                params.append("cr.in")

        # 2. Session Persistence (Sticky IP)
        if session_id:
            # sid.randomString allows keeping the same IP
            params.append(f"sid.{session_id}")
            # Set duration for sticky session
            params.append(f"intvlv.{duration}")

        if not params:
            return username

        # Join parameters with delimiter __ and separator ;
        # The base username must come first
        base_user = username.split("__")[0]
        param_string = ";".join(params)
        
        return f"{base_user}__{param_string}"

class StealthManager:
    """Manages browser stealth, user-agent rotation, and header spoofing."""
    
    # Fallback pool for 2026 (Chrome 140+)
    FALLBACK_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    ]

    # 2026 Persistent Identity: Static MacOS signature for proxy-less sessions
    PERSISTENT_MACOS_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"

    @classmethod
    def get_persistent_ua(cls) -> str:
        """Returns the fixed MacOS User-Agent for the entire session."""
        return cls.PERSISTENT_MACOS_UA

    @classmethod
    def get_random_ua(cls) -> str:
        if ua_generator:
            try:
                return ua_generator.random
            except Exception:
                pass
        return random.choice(cls.FALLBACK_USER_AGENTS)

    @classmethod
    def get_jitter_delay(cls, min_delay: float = 0.5, max_delay: float = 2.0) -> float:
        """Returns a randomized delay to mimic human reading speed."""
        return random.uniform(min_delay, max_delay)

    @classmethod
    def get_modern_headers(cls, user_agent: str) -> Dict[str, str]:
        """Generate headers including Client Hints to match the User-Agent."""
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",  # Added regional preference
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "User-Agent": user_agent,
            "Referer": "https://www.google.com/",  # Standard entry point
        }
        
        # 2026 Enterprise Feature: Dynamic Client Hints Generation
        # This prevents WAFs from detecting version mismatch between UA and Hints.
        if "Chrome/" in user_agent:
            try:
                # Extract full version and major version (e.g. 147.0.0.0 -> 147)
                match = re.search(r"Chrome/(\d+)\.", user_agent)
                major_version = match.group(1) if match else "147"
                
                platform = '"Windows"'
                if "Macintosh" in user_agent: platform = '"macOS"'
                elif "Linux" in user_agent: platform = '"Linux"'
                
                platform = '"macOS"'
                headers["sec-ch-ua-platform"] = platform
                
                # 2026 Refined Client Hints: Full Version List
                headers["sec-ch-ua-full-version-list"] = f'"Not(A:Brand";v="99", "Google Chrome";v="{major_version}.0.0.0", "Chromium";v="{major_version}.0.0.0"'
                headers["sec-ch-ua"] = f'"Not(A:Brand";v="99", "Google Chrome";v="{major_version}", "Chromium";v="{major_version}"'
                headers["sec-ch-ua-mobile"] = "?0"
            except Exception:
                headers["sec-ch-ua"] = '"Chromium";v="147", "Not(A:Brand";v="24", "Google Chrome";v="147"'
                headers["sec-ch-ua-mobile"] = "?0"
                headers["sec-ch-ua-platform"] = '"macOS"'
            
        return headers


 