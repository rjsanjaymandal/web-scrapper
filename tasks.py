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
PROJ_DIR = Path(__file__).parent

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Celery & Redis for Status
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"OK")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", "8080"))
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        logger.info(f"Background Health Server started on port {port}")
        server.serve_forever()
    except Exception as e:
        logger.warning(f"Health server could not start: {e}")

if not os.environ.get('CELERY_HEALTH_SERVER_STARTED'):
    os.environ['CELERY_HEALTH_SERVER_STARTED'] = '1'
    threading.Thread(target=start_health_server, daemon=True).start()

redis_url = os.environ.get('REDIS_URL')
redis_client = redis.Redis.from_url(redis_url) if redis_url else None

if redis_client:
    try:
        # Reconfigure Redis to be less aggressive about background saving
        # Default "save 60 1" causes continuous high IO and log spam
        redis_client.config_set("save", "900 1 300 10 60 10000")
        logger.info("Configured Redis background save intervals to be less aggressive.")
    except Exception as e:
        logger.warning(f"Could not configure Redis save intervals: {e}")

def set_status(msg, is_running=True, stats=None):
    """Update status for the dashboard."""
    data = {
        "message": msg, 
        "running": is_running, 
        "time": datetime.now().strftime("%H:%M:%S"),
        "stats": stats or {}
    }
    
    if redis_client:
        try:
            redis_client.set("scraper_status", json.dumps(data), ex=3600)
        except Exception as e:
            logger.error(f"Redis status update failed: {e}")
            
    db_set_status(data)
    
    log_triggers = ["Queued", "Scraping", "Page", "Started", "Finished", "Error", "High-Speed", "API", "Sitemap"]
    source = "SCRAPER"
    if stats and isinstance(stats, dict):
        source = stats.get("source", "SCRAPER")
    
    if is_running and any(t in msg for t in log_triggers):
        db_log("INFO", msg, source)
    elif not is_running and any(t in msg for t in ["Finished", "Complete", "Batch", "Found"]):
        db_log("SUCCESS", msg, source)
    elif "Error" in msg or "Failed" in msg:
        db_log("ERROR", msg, source)

    logger.info(f"STATUS UPDATE: {msg}")

def db_set_status(data):
    """Fallback status storage in Database"""
    import sqlite3
    import psycopg2
    
    try:
        db_url = os.environ.get('DATABASE_URL')
        is_sqlite = not db_url
        if db_url and db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)

        if is_sqlite:
            db_path = PROJ_DIR / 'scraper_local.db'
            conn = sqlite3.connect(db_path, timeout=15)
        else:
            conn = psycopg2.connect(db_url, connect_timeout=3)
            conn.autocommit = True
            
        cur = conn.cursor()
        val_json = json.dumps(data)
        
        if is_sqlite:
            cur.execute("INSERT OR REPLACE INTO system_status (id, key, value, updated_at) VALUES (1, 'scraper_status', ?, ?)", 
                       (val_json, datetime.now()))
        else:
            cur.execute("""
                INSERT INTO system_status (id, key, value, updated_at) 
                VALUES (1, 'scraper_status', %s, NOW())
                ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """, (val_json,))
            
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"DB status update failed: {e}")

def db_log(level, message, source=None):
    """Write an entry to the Dashboard Activity Log"""
    import sqlite3
    import psycopg2
    
    try:
        db_url = os.environ.get('DATABASE_URL')
        is_sqlite = not db_url
        if db_url and db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)

        if is_sqlite:
            db_path = PROJ_DIR / 'scraper_local.db'
            conn = sqlite3.connect(db_path, timeout=15)
        else:
            conn = psycopg2.connect(db_url, connect_timeout=3)
            conn.autocommit = True
            
        cur = conn.cursor()
        
        if is_sqlite:
            cur.execute("INSERT INTO scraper_logs (level, message, source, created_at) VALUES (?, ?, ?, ?)", 
                       (level, message, source, datetime.now()))
        else:
            cur.execute("INSERT INTO scraper_logs (level, message, source, created_at) VALUES (%s, %s, %s, NOW())", 
                       (level, message, source))
            
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"DB log write failed: {e}")

if not redis_url:
    logger.warning("REDIS_URL not found. Celery tasks will run locally.")
    celery_app = Celery('web_scraper_app')
    celery_app.conf.update(task_always_eager=True)
else:
    celery_app = Celery('web_scraper_app', broker=redis_url, backend=redis_url)
    celery_app.conf.update(
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        worker_concurrency=1,
        broker_connection_retry_on_startup=True,
    )

def _load_runtime_config():
    from scraper import load_config
    config = load_config()
    config.scheduler_enabled = False
    return config

# DEACTIVATED: High-Security / Browser-Heavy Targets
# JustDial, YellowPages, Google Footprints are strictly disabled to save memory and bypass WAFs.
DEACTIVATED_SOURCES = ["JustDial", "YellowPages", "Google", "IndiaMart", "Sulekha", "ClickIndia"]

@celery_app.task(name="tasks.scrape_category_task")
def scrape_category_task(city: str, category: str, source: str = None, use_business: bool = False):
    """
    Main entry point for scraping.
    Pivoted to High-Speed HTTP Scraper for official registries.
    """
    # Block heavy targets immediately
    if source in DEACTIVATED_SOURCES or (use_business and source is None):
        msg = f"Skipping {source or 'Business'} target - Deactivated (WAF protection/Resource heavy)."
        logger.warning(msg)
        set_status(msg, False)
        return {"status": "skipped", "reason": "deactivated"}

    set_status(
        f"Started: High-Speed Scraping {category} in {city}...",
        True,
        {"city": city, "category": category, "source": source or "Official"},
    )

    try:
        from polite_http_scraper import PoliteHTTPScraper
        from scraper import ContactScraper, load_config
    except Exception as e:
        set_status(f"Error: scraper startup failed: {e}", False)
        logger.error(f"Task startup failed: {e}")
        raise
    
    async def _run_scrape():
        config = load_config()
        scraper = ContactScraper(config)
        await scraper.init_db()
        
        try:
            # Use the new high-speed extraction methods
            # This bypasses Playwright/Puppeteer entirely for supported sources
            count = await scraper.scrape_category_fast(city, category, source)
            set_status(f"Success: Extracted {count} leads from {source or 'Official Registries'}", False)
            return {"status": "completed", "count": count}
        except Exception as e:
            set_status(f"Error: {str(e)}", False)
            logger.error(f"Task failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()

    return asyncio.run(_run_scrape())


@celery_app.task(name="tasks.fast_scrape_task")
def fast_scrape_task(source: str = None, max_concurrent: int = None):
    """Drains all open APIs and sitemaps for the 2 Lakh target."""
    set_status(
        "Started: Draining official APIs...",
        True,
        {"source": source or "ALL", "concurrency": max_concurrent},
    )

    try:
        from polite_http_scraper import PoliteHTTPScraper
        from scraper import load_config, ContactScraper
    except Exception as e:
        set_status(f"Error: fast scrape startup failed: {e}", False)
        logger.error(f"Fast scrape startup failed: {e}")
        raise
    
    async def _run_fast():
        config = load_config()
        set_status("Draining Official APIs (High Speed)...")
        
        scraper = ContactScraper(config)
        await scraper.init_db()
        
        try:
            # Optimized batch extraction for all cities and categories
            total = 0
            for city in config.cities:
                for cat in config.categories:
                    count = await scraper.scrape_category_fast(city, cat, source)
                    total += count
                    if count > 0:
                        set_status(f"Progress: Found {total} leads total...")
            
            set_status(f"Success: Drained {total} records from official APIs.", False)
            return {"status": "completed", "total": total}
        except Exception as e:
            set_status(f"Error: {e}", False)
            logger.error(f"Fast scrape failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()
            
    return asyncio.run(_run_fast())

@celery_app.task(name="tasks.export_data_task")
def export_data_task():
    from scraper import ContactScraper, load_config
    async def _run():
        scraper = ContactScraper(load_config())
        await scraper.init_db()
        try:
            return await scraper.export_to_csv()
        finally: await scraper.close()
    return asyncio.run(_run())
