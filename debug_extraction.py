import asyncio
import logging
from scrapers.base import ScraperRegistry
from stealth_utils import StealthManager
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RawDebug")

async def debug_extraction():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=StealthManager.get_random_ua(),
            ignore_https_errors=True
        )
        page = await context.new_page()
        
        # Test IndiaMart
        scraper = ScraperRegistry.get("INDIAMART")
        city, cat = "Ahmedabad", "Manufacturing"
        url = scraper.build_search_url(city, cat)
        logger.info(f"Targeting IndiaMart: {url}")
        
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5) # Wait for JS
        
        listings = await scraper.extract_listings(page, city, cat)
        logger.info(f"IndiaMart Raw Extracted: {len(listings)} leads")
        if listings:
            for l in listings[:3]:
                logger.info(f"Sample: {l}")
        
        # Test FOOTPRINT
        scraper = ScraperRegistry.get("FOOTPRINT")
        url = scraper.build_search_url(city, cat)
        logger.info(f"Targeting FOOTPRINT: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(3)
        listings = await scraper.extract_listings(page, city, cat)
        logger.info(f"FOOTPRINT Raw Extracted: {len(listings)} leads")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_extraction())
