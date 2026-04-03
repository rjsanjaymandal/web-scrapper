import asyncio
import argparse
import sys
from scraper import ContactScraper, load_config
import logging

# Setup minimal logging to console
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

async def test_scrape(city, category, source=None, limit=5):
    logger.info(f"--- Starting Test Scrape ---")
    logger.info(f"Target: {category} in {city}")
    if source:
        logger.info(f"Source: {source}")
    
    config = load_config()
    config.test_mode = True # Use visible browser for testing
    config.headless = False
    
    scraper = ContactScraper(config)
    
    # We don't necessarily need the DB for a pure extraction test, 
    # but we'll init it (it will fallback to SQLite if needed)
    await scraper.init_db()
    await scraper.init_browser()
    
    try:
        # Find the specific scraper
        scrapers_to_test = scraper.scrapers
        if source:
            scrapers_to_test = [s for s in scraper.scrapers if s.source_name.lower() == source.lower()]
        
        if not scrapers_to_test:
            logger.error(f"No scraper found for source: {source}")
            return

        for s in scrapers_to_test:
            logger.info(f"\nTesting Source: {s.source_name}")
            url = s.build_search_url(city, category)
            logger.info(f"URL: {url}")
            
            # Use the internal _extract_current_page instead of full save_to_db flow
            await scraper.page.goto(url, wait_until='networkidle')
            listings = await s.extract_listings(scraper.page)
            
            logger.info(f"Found {len(listings)} listings on page 1")
            
            for i, l in enumerate(listings[:limit]):
                print(f"\n[{i+1}] {l['name']}")
                print(f"    Phone: {l['phone']}")
                print(f"    Area:  {l['area']}")
                print(f"    URL:   {l['detail_url']}")

    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        await scraper.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Scraper Extraction')
    parser.add_argument('--city', type=str, default='Delhi', help='City to test')
    parser.add_argument('--cat', type=str, default='Insurance Agents', help='Category to test')
    parser.add_argument('--source', type=str, default=None, help='Source (JustDial, IndiaMart, ICICI)')
    parser.add_argument('--limit', type=int, default=5, help='Number of results to show')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(test_scrape(args.city, args.cat, args.source, args.limit))
    except KeyboardInterrupt:
        pass
