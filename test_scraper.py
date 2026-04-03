#!/usr/bin/env python3
"""
Test script for AMFI/IRDAI scraper.
Run locally to verify scraping works before deploying to Railway.
"""
import asyncio
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable test mode
os.environ['SCRAPER_TEST_MODE'] = 'true'

SCREENSHOTS_DIR = Path("screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)

async def test_amfi():
    """Test AMFI scraping."""
    from scraper import ContactScraper, load_config
    
    config = load_config()
    config.test_mode = True
    
    scraper = ContactScraper(config)
    await scraper.init_db()
    await scraper.init_browser()
    
    try:
        city = "Delhi"
        category = "Mutual-Fund-Agents"
        source_name = "AMFI"
        
        # Get AMFI scraper
        amfi_scraper = None
        for s in scraper.scrapers:
            if s.source_name == source_name:
                amfi_scraper = s
                break
        
        if not amfi_scraper:
            logger.error("AMFI scraper not found!")
            return
        
        url = amfi_scraper.build_search_url(city, category)
        logger.info(f"Testing AMFI URL: {url}")
        
        # Navigate to page
        await scraper.page.goto(url, timeout=60000, wait_until='networkidle')
        await asyncio.sleep(3)
        
        # Take screenshot of initial page
        await scraper.page.screenshot(path=str(SCREENSHOTS_DIR / "amfi_initial.png"))
        logger.info("Screenshot saved: amfi_initial.png")
        
        page_title = await scraper.page.title()
        logger.info(f"Page title: {page_title}")
        
        # Extract listings
        listings = await amfi_scraper.extract_listings(scraper.page, city, category)
        logger.info(f"Extracted {len(listings)} listings")
        
        # Take screenshot after extraction
        await scraper.page.screenshot(path=str(SCREENSHOTS_DIR / "amfi_after.png"))
        logger.info("Screenshot saved: amfi_after.png")
        
        if listings:
            logger.info("Sample listing:")
            logger.info(listings[0])
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        # Take screenshot on error
        try:
            await scraper.page.screenshot(path=str(SCREENSHOTS_DIR / "amfi_error.png"))
            logger.info("Error screenshot saved: amfi_error.png")
        except:
            pass
    finally:
        await scraper.close()


async def test_irdai():
    """Test IRDAI scraping."""
    from scraper import ContactScraper, load_config
    
    config = load_config()
    config.test_mode = True
    
    scraper = ContactScraper(config)
    await scraper.init_db()
    await scraper.init_browser()
    
    try:
        city = "Mumbai"
        category = "Insurance-Agents"
        source_name = "IRDAI"
        
        # Get IRDAI scraper
        irdai_scraper = None
        for s in scraper.scrapers:
            if s.source_name == source_name:
                irdai_scraper = s
                break
        
        if not irdai_scraper:
            logger.error("IRDAI scraper not found!")
            return
        
        url = irdai_scraper.build_search_url(city, category)
        logger.info(f"Testing IRDAI URL: {url}")
        
        # Navigate to page
        await scraper.page.goto(url, timeout=60000, wait_until='networkidle')
        await asyncio.sleep(3)
        
        page_title = await scraper.page.title()
        logger.info(f"Page title: {page_title}")
        
        # Extract listings
        listings = await irdai_scraper.extract_listings(scraper.page, city, category)
        logger.info(f"Extracted {len(listings)} listings")
        
        if listings:
            logger.info("Sample listing:")
            logger.info(listings[0])
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await scraper.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        test = sys.argv[1].lower()
        if test == 'amfi':
            asyncio.run(test_amfi())
        elif test == 'irdai':
            asyncio.run(test_irdai())
        else:
            print("Usage: python test_scraper.py [amfi|irdai]")
    else:
        print("Testing AMFI first...")
        asyncio.run(test_amfi())
        print("\n" + "="*50)
        print("Testing IRDAI...")
        asyncio.run(test_irdai())
