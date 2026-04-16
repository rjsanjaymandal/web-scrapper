import asyncio
import logging
import time
import os
from fast_scraper import fast_scrape_all, FastScraperConfig

# Setup environment for test (adjust as needed)
os.environ["HEADLESS"] = "True"
os.environ["MAX_CONCURRENT"] = "5" # Low for safe test, but uses V2 logic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BenchmarkV2")

async def run_benchmark():
    logger.info("Starting Engine V2 Benchmark...")
    
    # Configuration
    config_dict = {
        "cities": ["Ahmedabad"],
        "categories": ["Manufacturing"]
    }
    
    start_time = time.time()
    
    # Engine V2 utilizes pooled browsers and the new FOOTPRINT source
    total_leads = await fast_scrape_all(config_dict, config_dict["cities"], config_dict["categories"])
    duration = time.time() - start_time
    
    logger.info("=" * 40)
    logger.info(f"BENCHMARK COMPLETE")
    logger.info(f"Total Leads Ingested: {total_leads}")
    logger.info(f"Total Duration: {duration:.2f} seconds")
    if duration > 0:
        logger.info(f"Throughput: {total_leads / duration:.2f} leads/sec")
    logger.info("=" * 40)

if __name__ == "__main__":
    asyncio.run(run_benchmark())
