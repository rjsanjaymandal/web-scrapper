import random
from typing import Dict, List, Optional
from playwright.async_api import BrowserContext, Page
try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

class StealthManager:
    """Manages browser stealth, user-agent rotation, and header spoofing."""
    
    # Curated pool of modern User-Agents (Windows, Mac, Linux)
    # Focus on Chrome and Safari for consistency with Playwright's Chromium engine
    USER_AGENTS = [
        # Chrome Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        # Chrome MacOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        # Safari MacOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        # Firefox (Masked as Chrome-like for better compatibility with Playwright Chromium)
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    ]

    @classmethod
    def get_random_ua(cls) -> str:
        return random.choice(cls.USER_AGENTS)

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
            // Redundantly hide webdriver
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // Fix plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                ]
            });

            // Add languages
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            
            // Fix chrome object
            window.chrome = { runtime: {} };
            
            // Fix permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
        """)
