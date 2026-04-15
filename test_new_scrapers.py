import asyncio
import logging
from scraper import AMFIScraper, ContactScraper, load_config
from stealth_utils import StealthManager
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_amfi():
    logger.info("Testing AMFI Scraper Interaction with Stealth...")
    config = load_config()
    config.headless = True # Set to False locally to watch interaction
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.headless)
        
        user_agent = StealthManager.get_random_ua()
        extra_headers = StealthManager.get_modern_headers(user_agent)
        
        context = await browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers
        )
        
        # Apply stealth patches
        await StealthManager.apply_stealth(context)
        
        page = await context.new_page()

        
        scraper = AMFIScraper()
        url = scraper.build_search_url("Mumbai", "Mutual Fund")
        
        logger.info(f"Navigating to: {url}")
        await page.goto(url, wait_until='networkidle')
        
        logger.info("Extracting listings (with interaction)...")
        listings = await scraper.extract_listings(page, city="Mumbai", category="Mutual Fund")
        
        logger.info(f"Found {len(listings)} listings!")
        if listings:
            for l in listings[:3]:
                logger.info(f"Sample: {l['name']} | ARN: {l.get('arn')} | City: {l.get('city')}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_amfi())
