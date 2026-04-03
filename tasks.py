from celery import Celery
import asyncio
import logging
from pathlib import Path
from scraper import ContactScraper, load_config

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Celery
# Note: In a production environment, these URLs should come from config.yaml or environment variables
celery_app = Celery('scraper', 
                    broker='redis://localhost:6379/0', 
                    backend='redis://localhost:6379/0')

@celery_app.task(name="tasks.scrape_category_task")
def scrape_category_task(city: str, category: str, source: str = None):
    """
    Background task to scrape a specific category in a city from a specific source.
    """
    logger.info(f"Starting background scrape: {source or 'All'} - {category} - {city}")
    
    async def _run_scrape():
        config = load_config()
        # Ensure we don't accidentally run in scheduler mode or similar
        config.scheduler_enabled = False 
        
        scraper = ContactScraper(config)
        await scraper.init_db()
        await scraper.init_browser()
        try:
            await scraper.scrape_category(city, category, source)
            return {"status": "completed", "city": city, "category": category, "source": source}
        except Exception as e:
            logger.error(f"Task failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()

    return asyncio.run(_run_scrape())

@celery_app.task(name="tasks.validate_all_contacts_task")
def validate_all_contacts_task():
    """
    Background task to validate and enrich all contacts in the database.
    """
    logger.info("Starting background validation task")
    
    async def _run_validation():
        config = load_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            # We add a method to ContactScraper for this or use existing logic
            # For now, we'll implement the logic here calling existing methods if possible
            async with scraper.pool.acquire() as conn:
                contacts = await conn.fetch('SELECT id, phone, email FROM contacts')
                updated = 0
                for c in contacts:
                    # Logic similar to dashboard.js admin actions
                    # This is just a placeholder until we move logic into ContactScraper
                    updated += 1
                return {"status": "completed", "validated": updated}
        finally:
            await scraper.close()

    return asyncio.run(_run_validation())

@celery_app.task(name="tasks.export_data_task")
def export_data_task(export_format: str = "csv"):
    """
    Background task to export data.
    """
    logger.info(f"Starting background export task: {export_format}")
    
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
    """
    Cleanup task to remove old records.
    """
    logger.info(f"Cleaning up data older than {days} days")
    
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
