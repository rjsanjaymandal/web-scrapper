import asyncio
import logging
from dataclasses import asdict
from fast_scraper import ParallelScraper, FastScraperConfig
from scraper import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_jd():
    logger.info("Testing JustDial Protocol Resilience (H1 Toggle)...")
    config_dict = load_config()
    # Ensure H1 is forced for JD
    if "JUSTDIAL" not in config_dict.scraper_settings.get("force_http1_sources", []):
        config_dict.scraper_settings.setdefault("force_http1_sources", []).append("JUSTDIAL")
    
    # Convert dataclass to dict for FastScraperConfig
    config = FastScraperConfig(asdict(config_dict))
    config.headless = True
    config.max_concurrent = 1
    
    scraper_engine = ParallelScraper(config)
    await scraper_engine.init()
    
    try:
        # Test Delhi Chartered Accountants
        count = await scraper_engine.scrape_job("Delhi", "Chartered Accountants", "JUSTDIAL")
        logger.info(f"JustDial Test Finished. Extracted: {count} leads.")
        
        if count == 0 and "http2" in str(getattr(scraper_engine, 'last_error', '')).lower():
            logger.error("DARN! H2 Error still leaked through.")
        elif count > 0:
            logger.info("VICTORY! JustDial protocol block bypassed.")
            
    finally:
        await scraper_engine.close()

if __name__ == "__main__":
    asyncio.run(test_jd())
