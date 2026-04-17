import asyncio
import logging
import os
import sys
from typing import List, Dict

# Ensure current directory is in path for imports
sys.path.append(os.getcwd())

from scraper import load_config
from fast_scraper import ParallelScraper, FastScraperConfig
from redis_manager import RedisQueueManager

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    # 1. Load Configuration
    config_dict = load_config()
    redis_url = os.environ.get("REDIS_URL") or config_dict.scraper_settings.get("redis_url")
    
    if not redis_url:
        logger.error("REDIS_URL not found. Cannot start worker.")
        return

    # 2. Initialize Redis Manager
    queue = RedisQueueManager(redis_url)
    await queue.connect()

    # 3. Initialize Scraper Engine
    scraper_config = FastScraperConfig(config_dict.__dict__) # Convert dataclass to dict if needed, or pass directly
    engine = ParallelScraper(scraper_config)
    await engine.init()

    logger.info("👷 Worker Started. Waiting for tasks...")

    async def push_to_redis_buffer(valid_records: List[Dict], category: str, city: str, source: str):
        """Callback handler to push results to Redis instead of DB."""
        if valid_records:
            await queue.push_results(valid_records)
            logger.info(f"📤 Pushed {len(valid_records)} records to Redis buffer ({source} | {city})")

    try:
        while True:
            # 4. Consume Task
            task = await queue.pop_task(timeout=30)
            if not task:
                # No tasks found, keep waiting or sleep briefly
                continue
            
            logger.info(f"📥 Received Task: {task}")
            
            try:
                # 5. Execute Scrape
                await engine.scrape_job(
                    city=task["city"],
                    category=task["category"],
                    source_name=task["source"],
                    page_num=task.get("page", 1),
                    results_handler=push_to_redis_buffer
                )
            except Exception as e:
                logger.error(f"❌ Worker failed job {task}: {e}")
                # Optional: Push task back to queue with retry count logic
                
    except asyncio.CancelledError:
        logger.info("Worker shutting down...")
    finally:
        await engine.close()
        await queue.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
