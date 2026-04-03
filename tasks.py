from celery import Celery
import asyncio
import logging
import os
import sys
import json
import redis
from datetime import datetime
from pathlib import Path

# Fix for Railway/Docker: Ensure the current directory is in the Python path
sys.path.append(os.getcwd())

# Heavy imports are now inside the task to ensure registration never fails
# from scraper import ContactScraper, load_config

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
    celery_app = Celery('web_scraper_app')
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True
    )
else:
    celery_app = Celery('web_scraper_app', 
                        broker=redis_url, 
                        backend=redis_url)
    celery_app.conf.update(
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        worker_concurrency=int(os.environ.get('CELERY_WORKER_CONCURRENCY', '1')),
        worker_pool=os.environ.get('CELERY_WORKER_POOL', 'solo'),
        broker_connection_retry_on_startup=True,
    )


def _load_runtime_config():
    from scraper import load_config

    config = load_config()
    config.scheduler_enabled = False
    return config

@celery_app.task(name="tasks.scrape_category_task")
def scrape_category_task(city: str, category: str, source: str = None, use_business: bool = False):
    """
    Background task to scrape a specific category in a city from a specific source.
    use_business: If True, use business directories (JustDial, etc). If False (default), use official sources (AMFI, IRDAI, ICAI).
    """
    from scraper import ContactScraper
    set_status(f"🚀 Scraping {category} in {city}...")
    
    async def _run_scrape():
        config = _load_runtime_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            await scraper.scrape_category(city, category, source, use_business)
            set_status(f"✅ Finished: {category} in {city}", False)
            return {"status": "completed", "city": city, "category": category, "source": source, "use_business": use_business}
        except Exception as e:
            set_status(f"❌ Failed: {str(e)}", False)
            logger.error(f"Task failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()

    return asyncio.run(_run_scrape())


@celery_app.task(name="tasks.scrape_all_task")
def scrape_all_task(source: str = None, use_business: bool = False):
    from scraper import ContactScraper

    async def _run_batch():
        config = _load_runtime_config()
        jobs = [(city, category) for city in config.cities for category in config.categories]
        scraper = ContactScraper(config)
        results = []

        await scraper.init_db()
        try:
            total_jobs = len(jobs)
            for index, (city, category) in enumerate(jobs, start=1):
                set_status(f"🚀 [{index}/{total_jobs}] Scraping {category} in {city}...")
                try:
                    await scraper.scrape_category(city, category, source, use_business)
                    results.append({"city": city, "category": category, "status": "completed"})
                except Exception as exc:
                    logger.error(f"Batch item failed for {category} in {city}: {exc}")
                    results.append({"city": city, "category": category, "status": "failed", "error": str(exc)})

            failures = [item for item in results if item["status"] == "failed"]
            if failures:
                set_status(f"⚠️ Batch finished with {len(failures)} failures", False)
            else:
                set_status(f"✅ Batch finished ({len(results)} jobs)", False)

            return {
                "status": "completed",
                "jobs": len(results),
                "failures": len(failures),
                "results": results,
                "source": source,
                "use_business": use_business,
            }
        finally:
            await scraper.close()

    return asyncio.run(_run_batch())

@celery_app.task(name="tasks.validate_all_contacts_task")
def validate_all_contacts_task():
    from scraper import ContactScraper
    set_status("🔍 Validating all contacts...")
    async def _run_validation():
        config = _load_runtime_config()
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
    from scraper import ContactScraper
    async def _run_export():
        config = _load_runtime_config()
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
    from scraper import ContactScraper
    async def _run_cleanup():
        config = _load_runtime_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        try:
            async with scraper.pool.acquire() as conn:
                res = await conn.execute(
                    "DELETE FROM contacts WHERE scraped_at < NOW() - make_interval(days => $1)",
                    days,
                )
                return {"status": "completed", "deleted": res}
        finally:
            await scraper.close()
    return asyncio.run(_run_cleanup())
