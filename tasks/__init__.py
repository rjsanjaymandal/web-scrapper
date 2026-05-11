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
# In tasks/__init__.py, the project root is the parent directory
PROJ_DIR = Path(__file__).parent.parent

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
    celery_app = Celery('web_scraper_app', include=['tasks.api_scraper', 'tasks.sitemap_crawler', 'tasks.profile_scraper'])
    celery_app.conf.update(task_always_eager=True)
else:
    celery_app = Celery('web_scraper_app', 
                        broker=redis_url, 
                        backend=redis_url,
                        include=['tasks.api_scraper', 'tasks.sitemap_crawler', 'tasks.profile_scraper'])
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
# Google Footprints and high-WAF business targets are strictly limited.
DEACTIVATED_SOURCES = ["Google", "IndiaMart", "TradeIndia"]

@celery_app.task(name="tasks.scrape_category_task", time_limit=1800, soft_time_limit=1500)
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

    from scrape_state import claim_scrape_job, finish_scrape_job

    claimed, reason, token = claim_scrape_job(city, category, source)
    if not claimed:
        msg = f"Skipped: {category} in {city} ({reason})"
        logger.info(msg)
        set_status(msg, False)
        return {"status": "skipped", "reason": reason}

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
            finish_scrape_job(city, category, source, token=token, count=count, success=True)
            set_status(f"Success: Extracted {count} leads from {source or 'Official Registries'}", False)
            return {"status": "completed", "count": count}
        except Exception as e:
            finish_scrape_job(
                city,
                category,
                source,
                token=token,
                count=0,
                success=False,
                error=str(e),
            )
            set_status(f"Error: {str(e)}", False)
            logger.error(f"Task failed: {e}")
            return {"status": "failed", "error": str(e)}
        finally:
            await scraper.close()

    return asyncio.run(_run_scrape())


@celery_app.task(name="tasks.fast_scrape_task", time_limit=3600, soft_time_limit=3300)
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
        from scrape_state import claim_scrape_job, finish_scrape_job
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
            skipped = 0
            for city in config.cities:
                for cat in config.categories:
                    claimed, reason, token = claim_scrape_job(city, cat, source)
                    if not claimed:
                        skipped += 1
                        logger.info(f"Skipping: {cat} in {city} ({reason})")
                        continue

                    try:
                        count = await scraper.scrape_category_fast(city, cat, source)
                        finish_scrape_job(city, cat, source, token=token, count=count, success=True)
                    except Exception as scrape_error:
                        finish_scrape_job(
                            city,
                            cat,
                            source,
                            token=token,
                            count=0,
                            success=False,
                            error=str(scrape_error),
                        )
                        logger.error(f"Fast scrape failed for {cat} in {city}: {scrape_error}")
                        continue

                    total += count
                    if count > 0:
                        set_status(f"Progress: Found {total} leads total...")
            
            set_status(f"Success: Drained {total} records from official APIs. Skipped {skipped} recent jobs.", False)
            return {"status": "completed", "total": total, "skipped": skipped}
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


@celery_app.task(name="tasks.direct_scrape_task", time_limit=1800, soft_time_limit=1600)
def direct_scrape_task(source: str = None, city: str = None, category: str = None):
    """
    Direct scraping without proxies - optimized for government sites.
    Uses polite HTTP fetching to avoid blocking.
    """
    set_status(
        f"Started: Direct scraping {source or 'Gov Sites'}...",
        True,
        {"source": source, "city": city, "category": category},
    )
    
    try:
        from direct_scraper import (
            DirectPoliteFetcher, 
            SEBIDirectScraper, 
            ICAIDirectScraper,
            MCADirectScraper,
            AMFIDirectScraper,
            NSEDirectScraper,
            get_scraper
        )
    except Exception as e:
        set_status(f"Error: Could not import direct scraper: {e}", False)
        logger.error(f"Direct scrape import failed: {e}")
        return {"status": "failed", "error": str(e)}
    
    try:
        fetcher = DirectPoliteFetcher()
        
        # Map source names to scraper classes
        scraper_map = {
            "SEBI": SEBIDirectScraper,
            "ICAI": ICAIDirectScraper,
            "MCA": MCADirectScraper,
            "AMFI": AMFIDirectScraper,
            "NSE": NSEDirectScraper,
        }
        
        source_upper = (source or "SEBI").upper()
        scraper_class = scraper_map.get(source_upper)
        
        if not scraper_class:
            set_status(f"Error: Unknown source {source}", False)
            return {"status": "failed", "error": f"Unknown source: {source}"}
        
        scraper = scraper_class(fetcher)
        results = scraper.scrape(city=city, category=category)
        
        if results:
            # Save to database
            from processing import ProcessingHandler
            from bulk_writer import BulkWriter
            
            handler = ProcessingHandler()
            processed = []
            
            for contact in results:
                try:
                    cleaned = handler.process_contact(contact)
                    if cleaned.get('phone_clean') or cleaned.get('email_valid'):
                        processed.append(cleaned)
                except Exception as proc_err:
                    logger.warning(f"Processing error: {proc_err}")
            
            if processed:
                # Save to database
                try:
                    from scraper import ContactScraper, load_config
                    async def save_to_db():
                        db_scraper = ContactScraper(load_config())
                        await db_scraper.init_db()
                        try:
                            count = await db_scraper.save_contacts(processed)
                            return count
                        finally:
                            await db_scraper.close()
                    
                    saved_count = asyncio.run(save_to_db())
                    set_status(f"Success: Extracted {len(results)}, saved {saved_count} from {source}", False)
                    return {"status": "completed", "extracted": len(results), "saved": saved_count}
                except Exception as db_err:
                    logger.error(f"Database save error: {db_err}")
                    set_status(f"Extracted {len(results)} but failed to save: {db_err}", False)
                    return {"status": "completed", "extracted": len(results), "saved": 0, "db_error": str(db_err)}
        else:
            set_status(f"No results from {source} via direct scraping", False)
            return {"status": "completed", "extracted": 0, "saved": 0}
            
    except Exception as e:
        set_status(f"Error: {e}", False)
        logger.error(f"Direct scrape failed: {e}")
        return {"status": "failed", "error": str(e)}


@celery_app.task(name="tasks.direct_gov_scrape_batch", time_limit=3600, soft_time_limit=3300)
def direct_gov_scrape_batch():
    """
    Batch direct scraping for all government sites.
    No proxies - polite HTTP fetching for regulatory sites.
    """
    set_status("Started: Direct Gov Sites Batch...", True, {"source": "GOVERNMENT"})
    
    try:
        from direct_scraper import (
            DirectPoliteFetcher,
            SEBIDirectScraper,
            ICAIDirectScraper,
            MCADirectScraper,
            AMFIDirectScraper,
            NSEDirectScraper,
        )
    except Exception as e:
        set_status(f"Error: {e}", False)
        return {"status": "failed", "error": str(e)}
    
    try:
        config_cities = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad", "Pune", 
                        "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
        
        gov_sources = [
            ("SEBI", SEBIDirectScraper, "Investment Advisors"),
            ("ICAI", ICAIDirectScraper, "Chartered Accountants"),
            ("NSE", NSEDirectScraper, "Stock Brokers"),
            ("MCA", MCADirectScraper, "Company Secretaries"),
            ("AMFI", AMFIDirectScraper, "Mutual Fund Agents"),
        ]
        
        fetcher = DirectPoliteFetcher()
        total_results = 0
        total_saved = 0
        
        for source_name, scraper_class, category in gov_sources:
            set_status(f"Scraping {source_name}...", True, {"source": source_name})
            
            scraper = scraper_class(fetcher)
            
            try:
                results = scraper.scrape(city=None, category=category)
                total_results += len(results)
                
                if results:
                    set_status(f"{source_name}: Found {len(results)} records...", True, {"source": source_name})
                    
                    # Process and save results
                    from processing import ProcessingHandler
                    handler = ProcessingHandler()
                    processed = []
                    
                    for contact in results:
                        try:
                            cleaned = handler.process_contact(contact)
                            if cleaned.get('phone_clean') or cleaned.get('email_valid'):
                                processed.append(cleaned)
                        except:
                            continue
                    
                    if processed:
                        try:
                            from scraper import ContactScraper, load_config
                            async def save_batch():
                                db = ContactScraper(load_config())
                                await db.init_db()
                                try:
                                    count = await db.save_contacts(processed)
                                    return count
                                finally:
                                    await db.close()
                            
                            saved = asyncio.run(save_batch())
                            total_saved += saved
                        except Exception as db_err:
                            logger.warning(f"DB save error for {source_name}: {db_err}")
                            
            except Exception as src_err:
                logger.error(f"Source {source_name} failed: {src_err}")
                continue
        
        set_status(f"Gov Batch Complete: {total_results} found, {total_saved} saved", False)
        return {"status": "completed", "extracted": total_results, "saved": total_saved}
        
    except Exception as e:
        set_status(f"Error: {e}", False)
        logger.error(f"Direct gov batch failed: {e}")
        return {"status": "failed", "error": str(e)}
