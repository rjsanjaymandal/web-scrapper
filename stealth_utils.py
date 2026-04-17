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
    
    # Fallback pool for 2026 (Chrome 140+)
    FALLBACK_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
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
                
                headers["sec-ch-ua"] = f'"Not(A:Brand";v="99", "Google Chrome";v="{major_version}", "Chromium";v="{major_version}"'
                headers["sec-ch-ua-mobile"] = "?0"
                headers["sec-ch-ua-platform"] = platform
            except Exception:
                headers["sec-ch-ua"] = '"Chromium";v="147", "Not(A:Brand";v="24", "Google Chrome";v="147"'
                headers["sec-ch-ua-mobile"] = "?0"
                headers["sec-ch-ua-platform"] = '"Windows"'
            
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

            // 3. Mask Hardware signatures (Hide lowvCPU/lowRAM server profile)
            // 2026 Standards: High-entropy randomization
            const concurrency = [4, 8, 12, 16][Math.floor(Math.random() * 4)];
            const memory = [8, 16, 32][Math.floor(Math.random() * 3)];
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => concurrency });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => memory });
            
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
