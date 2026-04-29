import asyncio
import logging
import os
import yaml
from datetime import datetime
from scraper import ContactScraper, load_config

# Configure Logging for Production (Railway)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EnterpriseAutomator")

async def run_enterprise_cycle():
    logger.info("Starting Enterprise Automation Cycle (100+ Cities)...")
    
    # 1. Load Configuration
    config = load_config()
    cities = config.cities
    categories = config.categories
    
    start_time = datetime.now()
    logger.info(f"Target: {len(cities)} Cities | {len(categories)} Categories")
    
    scraper = ContactScraper(config)
    await scraper.init_db()
    
    # 3. Execute High-Speed Scraping Suite
    try:
        total_leads = 0
        for city in cities:
            for cat in categories:
                count = await scraper.scrape_category_fast(city, cat, None)
                total_leads += count

        duration = datetime.now() - start_time
        logger.info("=" * 50)
        logger.info("AUTOMATION CYCLE COMPLETE")
        logger.info(f"Total Leads Discovered: {total_leads}")
        logger.info(f"Total Duration: {duration}")
        logger.info(f"Average Speed: {total_leads / max(duration.total_seconds(), 1):.2f} leads/sec")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Automation Cycle crashed: {e}")
        raise
    finally:
        await scraper.close()

if __name__ == "__main__":
    asyncio.run(run_enterprise_cycle())
