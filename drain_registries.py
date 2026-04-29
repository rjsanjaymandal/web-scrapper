import asyncio
import logging
import os
from datetime import datetime
from scraper import ContactScraper, load_config
from scrapers_registry import ScraperRegistry

# Optimized for 2026: Low RAM, Direct Official API Draining
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RegistryDrainer")

async def drain_all_registries():
    """
    High-Speed Registry Drainer.
    Targets official associations to pull 100k-300k leads with 0 proxies.
    """
    logger.info("🌊 STARTING OFFICIAL REGISTRY DRAIN (Weekend Mode)")
    
    config = load_config()
    # Ensure high speed for this specific task
    config.max_concurrent = int(os.environ.get("DRAIN_CONCURRENT", 10))
    
    scraper = ContactScraper(config)
    await scraper.init_db()
    
    total_leads = 0
    
    # Priority 1: AMFI (Mutual Fund Distributors) ~100,000+ leads
    # We loop through major cities to get a granular drain
    amfi_cities = [
        "mumbai", "delhi", "bangalore", "hyderabad", "ahmedabad", 
        "chennai", "kolkata", "pune", "jaipur", "lucknow", "surat",
        "kanpur", "nagpur", "indore", "thane", "bhopal", "patna", "vadodara"
    ]
    
    logger.info("📍 Phase 1: Draining AMFI Mutual Fund Registry...")
    for city in amfi_cities:
        try:
            count = await scraper.scrape_category_fast(city, "mutual-fund-agents", "AMFI")
            total_leads += count
            logger.info(f"✅ AMFI {city}: +{count} leads (Running Total: {total_leads})")
            await asyncio.sleep(2) # Minimal delay for official APIs
        except Exception as e:
            logger.error(f"Error draining AMFI in {city}: {e}")

    # Priority 2: IBBI (Insolvency Professionals) ~5,000+ leads
    logger.info("📍 Phase 2: Draining IBBI Insolvency Registry...")
    try:
        count = await scraper.scrape_category_fast("all", "insolvency-professionals", "IBBI")
        total_leads += count
        logger.info(f"✅ IBBI: +{count} leads (Running Total: {total_leads})")
    except Exception as e:
        logger.error(f"Error draining IBBI: {e}")

    # Priority 3: SEBI (Investment Advisors) ~2,000+ leads
    logger.info("📍 Phase 3: Draining SEBI RIA Registry...")
    try:
        count = await scraper.scrape_category_fast("all", "sebi-advisor", "SEBI")
        total_leads += count
        logger.info(f"✅ SEBI: +{count} leads (Running Total: {total_leads})")
    except Exception as e:
        logger.error(f"Error draining SEBI: {e}")

    # Priority 4: Official sitemaps and directories
    # (Bar Councils, Regional CA directories)
    # These often have 50k+ leads across multiple pages
    
    logger.info("=" * 50)
    logger.info(f"🏆 DRAIN COMPLETE: Found {total_leads} verified registry leads.")
    logger.info(f"💾 All data saved to PostgreSQL: {os.environ.get('DATABASE_URL', 'local.db').split('@')[-1]}")
    logger.info("=" * 50)
    
    await scraper.close()

if __name__ == "__main__":
    asyncio.run(drain_all_registries())
