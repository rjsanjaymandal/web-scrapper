import random
import logging
from typing import Dict, List, Optional
from playwright.async_api import BrowserContext, Page
try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

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
    
    # Fallback pool if fake-useragent fails
    FALLBACK_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    ]

    @classmethod
    def get_random_ua(cls) -> str:
        if ua_generator:
            try:
                return ua_generator.random
            except Exception:
                pass
        return random.choice(cls.FALLBACK_USER_AGENTS)

    @classmethod
    def get_modern_headers(cls, user_agent: str) -> Dict[str, str]:
        """Generate headers including Client Hints to match the User-Agent."""
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "User-Agent": user_agent,
        }
        
        # Add basic Client Hints if it's a Chrome User-Agent
        if "Chrome" in user_agent:
            headers["sec-ch-ua"] = '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'
            headers["sec-ch-ua-mobile"] = "?0"
            headers["sec-ch-ua-platform"] = '"Windows"' if "Windows" in user_agent else '"macOS"'
            
        return headers

    @classmethod
    async def apply_stealth(cls, context: BrowserContext):
        """Apply playwright-stealth patches and custom evasions."""
        if stealth_async:
            await stealth_async(context)
        
        # Additional custom evasions that might not be in the library
        for page in context.pages:
            await cls.apply_stealth_to_page(page)

    @classmethod
    async def apply_stealth_to_page(cls, page: Page):
        """Apply stealth patches to a specific page."""
        await page.add_init_script("""
            // 1. Hide Webdriver
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // 2. Fix Plugins & MimeTypes
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                ]
            });

            // 3. Mask Hardware signatures (Hide 8vCPU/8GB server profile)
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
            
            // 4. Canvas Fingerprint Jitter
            const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
            CanvasRenderingContext2D.prototype.getImageData = function(x, y, w, h) {
                const imageData = originalGetImageData.apply(this, arguments);
                // Subtly jitter one pixel to change the resulting hash without breaking UI
                if (imageData.data.length > 0) {
                    imageData.data[0] = imageData.data[0] + (Math.random() > 0.5 ? 1 : -1);
                }
                return imageData;
            };

            // 5. Fix Chrome Runtime & Permissions
            window.chrome = { runtime: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
            
            // 6. Audio Context Spoofing
            const originalGetByteFrequencyData = AudioAnalyserNode.prototype.getByteFrequencyData;
            AudioAnalyserNode.prototype.getByteFrequencyData = function(array) {
                originalGetByteFrequencyData.apply(this, arguments);
                for (let i = 0; i < 5; i++) {
                    array[i] = array[i] + (Math.random() > 0.5 ? 1 : -1);
                }
            };
        """)
