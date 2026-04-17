import asyncio
import logging
from scraper import load_config, TradeIndiaScraper, IndiaMartScraper, YellowPagesScraper
from fast_scraper import ParallelScraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VERIFY")

async def test_b2b():
    config = load_config()
    
    from fast_scraper import ParallelScraper, FastScraperConfig
    import dataclasses
    
    # Adapt main Config to FastScraperConfig
    config_dict = dataclasses.asdict(config)
    fs_config = FastScraperConfig(config_dict)
    
    scraper_engine = ParallelScraper(fs_config)
    await scraper_engine.init()
    
    targets = [
        ("TRADEINDIA", "Chartered Accountants", "Delhi"),
        ("INDIAMART", "Manufacturing", "Ahmedabad"),
        ("YELLOWPAGES", "Software Companies", "Bangalore")
    ]
    
    for source, cat, city in targets:
        logger.info(f"--- Testing {source} ---")
        try:
            # signature is scrape_job(city, category, source_name)
            leads = await scraper_engine.scrape_job(city, cat, source)
            logger.info(f"RESULT: {source} returned {leads} leads.")
        except Exception as e:
            logger.error(f"FAILED: {source} errored: {e}")
            
    await scraper_engine.close()

if __name__ == "__main__":
    asyncio.run(test_b2b())
