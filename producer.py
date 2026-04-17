import asyncio
import logging
import os
import sys
from typing import List

# Ensure current directory is in path for imports
sys.path.append(os.getcwd())

from scraper import load_config
from scrapers_registry import ScraperRegistry
from redis_manager import RedisQueueManager

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    # 1. Load Configuration
    config = load_config()
    redis_url = os.environ.get("REDIS_URL") or config.scraper_settings.get("redis_url")
    
    if not redis_url:
        logger.error("REDIS_URL not found. Cannot start producer.")
        return

    # 2. Initialize Redis Manager
    queue = RedisQueueManager(redis_url)
    await queue.connect()

    try:
        cities = config.cities
        categories = config.categories
        max_pages = config.max_pages
        
        logger.info(f"Seeding tasks for {len(cities)} cities and {len(categories)} categories...")
        
        task_count = 0
        for city in cities:
            for category in categories:
                # Find all sources mapped to this category
                sources = ScraperRegistry.get_all_sources_for_category(category)
                
                for source in sources:
                    # Seed tasks for the requested number of pages
                    for page in range(1, max_pages + 1):
                        await queue.push_task(
                            source=source,
                            city=city,
                            category=category,
                            page=page
                        )
                        task_count += 1
        
        logger.info(f"✅ Producer Phase Complete: Seeded {task_count} tasks into 'scraper:tasks'.")
        
        # Log queue depth
        depth = await queue.get_queue_depth()
        logger.info(f"Current Queue Depth: {depth}")

    except Exception as e:
        logger.exception(f"Producer failed: {e}")
    finally:
        await queue.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
