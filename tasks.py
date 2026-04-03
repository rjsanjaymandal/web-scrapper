from celery import Celery
import asyncio
import logging
import os
import json
import redis
from datetime import datetime
from pathlib import Path
from scraper import ContactScraper, load_config

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Celery & Redis for Status
redis_url = os.environ.get('REDIS_URL')
redis_client = redis.Redis.from_url(redis_url) if redis_url else None

def set_status(msg, is_running=True):
    if redis_client:
        try:
            data = {"message": msg, "running": is_running, "time": datetime.now().strftime("%H:%M:%S")}
            redis_client.set("scraper_status", json.dumps(data), ex=3600)
        except Exception as e:
            logger.error(f"Redis status update failed: {e}")
    logger.info(f"STATUS UPDATE: {msg}")

if not redis_url:
    logger.warning("REDIS_URL not found. Celery tasks will run locally (always_eager).")
    celery_app = Celery('scraper')
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True
    )
else:
    celery_app = Celery('scraper', 
                        broker=redis_url, 
                        backend=redis_url)

@celery_app.task(name="tasks.scrape_category_task")
def scrape_category_task(city: str, category: str, source: str = None):
    """
    Background task to scrape a specific category in a city from a specific source.
    """
    set_status(f"🚀 Scraping {category} in {city}...")
    
    async def _run_scrape():
        config = load_config()
        config.scheduler_enabled = False 
        
        scraper = ContactScraper(config)
        await scraper.init_db()
        await scraper.init_browser()
        try:
            await scraper.scrape_category(city, category, source)
            set_status(f"✅ Finished: {category} in {city}", False)
            return {"status": "completed", "city": city, "category": category, "source": source}
        except Exception as e:
            set_status(f"❌ Failed: {str(e)}", False)
            logger.error(f"Task failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()

    return asyncio.run(_run_scrape())

@celery_app.task(name="tasks.validate_all_contacts_task")
def validate_all_contacts_task():
    set_status("🔍 Validating all contacts...")
    async def _run_validation():
        config = load_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            async with scraper.pool.acquire() as conn:
                contacts = await conn.fetch('SELECT id, phone, email FROM contacts')
                set_status(f"✅ Validation finished ({len(contacts)} records)", False)
                return {"status": "completed", "validated": len(contacts)}
        finally:
            await scraper.close()
    return asyncio.run(_run_validation())

@celery_app.task(name="tasks.export_data_task")
def export_data_task(export_format: str = "csv"):
    async def _run_export():
        config = load_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            filename = await scraper.export_to_csv()
            return {"status": "completed", "filename": str(filename)}
        finally:
            await scraper.close()
    return asyncio.run(_run_export())

@celery_app.task(name="tasks.cleanup_old_data_task")
def cleanup_old_data_task(days: int = 90):
    async def _run_cleanup():
        config = load_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            async with scraper.pool.acquire() as conn:
                res = await conn.execute("DELETE FROM contacts WHERE scraped_at < NOW() - INTERVAL '$1 days'", days)
                return {"status": "completed", "deleted": res}
        finally:
            await scraper.close()
    return asyncio.run(_run_cleanup())
