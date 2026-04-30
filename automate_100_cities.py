import asyncio
import logging
import os
import yaml
from datetime import datetime
from scraper import ContactScraper, load_config
from scrape_state import claim_scrape_job, finish_scrape_job

# Configure Logging for Production (Railway)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EnterpriseAutomator")

async def run_enterprise_cycle():
    logger.info("🚀 Enterprise Automation Engine: INFINITE MODE ACTIVATED")
    
    while True:
        # 1. Load/Reload Configuration every cycle
        config = load_config()
        cities = config.cities
        categories = config.categories
        cycle_delay = getattr(config, 'cycle_delay', 3600) # Default to 1 hour
        
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info(f"🔄 NEW AUTOMATION CYCLE STARTED AT {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📍 Targets: {len(cities)} Cities | 🏷️ Categories: {len(categories)}")
        logger.info(f"⏳ Cycle Interval: {cycle_delay}s | 🛡️ Stealth: High")
        logger.info("=" * 60)
        
        scraper = ContactScraper(config)
        try:
            await scraper.init_db()
            total_leads = 0
            
            # Shuffle cities/categories for more organic-looking traffic
            import random
            shuffled_cities = list(cities)
            random.shuffle(shuffled_cities)
            
            for city in shuffled_cities:
                shuffled_cats = list(categories)
                random.shuffle(shuffled_cats)
                
                for cat in shuffled_cats:
                    claimed, reason, token = claim_scrape_job(city, cat)
                    if not claimed:
                        logger.info(f"Skipping: {cat} in {city} ({reason})")
                        continue

                    logger.info(f"Processing: {cat} in {city}...")
                    try:
                        count = await scraper.scrape_category_fast(city, cat, None)
                        finish_scrape_job(city, cat, token=token, count=count, success=True)
                        total_leads += count
                    except Exception as task_error:
                        finish_scrape_job(
                            city,
                            cat,
                            token=token,
                            count=0,
                            success=False,
                            error=str(task_error),
                        )
                        logger.error(f"Task failed for {cat} in {city}: {task_error}")
                        continue
                    
                    # 2026 Verification: Log current DB state to confirm storage is working
                    try:
                        db_stats = await scraper.get_stats()
                        logger.info(f"📊 [DATABASE VERIFICATION] Total leads in system: {db_stats['total_contacts']} (+{count} this task)")
                    except Exception as de:
                        logger.warning(f"Could not fetch DB stats: {de}")
                    
                    # Politeness: Small jittered sleep between tasks (5-15s)
                    # This prevents slamming multiple targets in a tight sequence
                    task_delay = random.uniform(5.0, 15.0)
                    await asyncio.sleep(task_delay)

            duration = datetime.now() - start_time
            logger.info("=" * 50)
            logger.info("CYCLE SUMMARY")
            logger.info(f"Leads Found: {total_leads}")
            logger.info(f"Duration: {duration}")
            logger.info(f"Sleeping for {cycle_delay}s before next cycle...")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"⚠️ Cycle encountered an error: {e}")
            logger.info("Restarting loop in 60s...")
            await asyncio.sleep(60)
            continue
        finally:
            await scraper.close()
            
        # Full Cycle Delay
        logger.info(f"🛌 Cycle complete. Entering persistent sleep for {cycle_delay}s...")
        
        # Heartbeat logic for 2026: Log every 10 mins during sleep to show we are alive
        sleep_start = datetime.now()
        while (datetime.now() - sleep_start).total_seconds() < cycle_delay:
            remaining = cycle_delay - (datetime.now() - sleep_start).total_seconds()
            sleep_chunk = min(600, remaining) # 10 mins or remaining
            if sleep_chunk > 0:
                await asyncio.sleep(sleep_chunk)
                if remaining > 600:
                    logger.info(f"💓 Heartbeat: Automator alive. Waiting for next cycle ({int(remaining/60)}m remaining)...")
            else:
                break

if __name__ == "__main__":
    try:
        asyncio.run(run_enterprise_cycle())
    except KeyboardInterrupt:
        logger.info("Manual shutdown received.")
