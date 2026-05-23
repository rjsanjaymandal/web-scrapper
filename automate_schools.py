#!/usr/bin/env python3
"""
MaysanLabs Web Scraper: Continuous School Contact Automator (2026 Edition)
Runs infinitely, shuffling configured cities, crawling and harvesting school contact leads
city by city, and saving them with O(1) deduplication and dual-write PG synchronization.
Windows-safe CP1252 ASCII output format.
"""

import os
import sys
import yaml
import random
import asyncio
import logging
from datetime import datetime

# Setup Windows-safe ASCII logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SchoolAutomator")

# Ensure project root is in path
sys.path.append(os.getcwd())

from scraper import ContactScraper, load_config
from direct_scraper import SchoolDirectScraper, DirectPoliteFetcher
from processing import ProcessingHandler
from scrape_state import claim_scrape_job, finish_scrape_job

async def run_school_automation():
    logger.info("=" * 60)
    logger.info("      *** STARTING CONTINUOUS SCHOOL CONTACT AUTOMATOR (INFINITE MODE) ***      ")
    logger.info("=" * 60)

    cycle_count = 0
    total_leads_saved = 0

    while True:
        cycle_count += 1
        config = load_config()
        cities = list(getattr(config, "cities", []) or [])
        
        if not cities:
            cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Lucknow"]
            logger.warning(f"No cities found in config.yaml. Using default list of {len(cities)} major cities.")

        # Jittered delays configuration
        cycle_delay = int(os.environ.get("SCHOOLS_CYCLE_DELAY", "1800"))  # Default 30 mins sleep between cycles
        task_delay_min = float(os.environ.get("SCHOOLS_DELAY_MIN", "15.0"))
        task_delay_max = float(os.environ.get("SCHOOLS_DELAY_MAX", "30.0"))

        start_time = datetime.now()
        logger.info(f"\n[~] CYCLE #{cycle_count} STARTED AT {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"    Targets: {len(cities)} Shuffled Cities | Category: Schools")
        logger.info(f"    Task Jitter: {task_delay_min}s - {task_delay_max}s | Cycle Delay: {cycle_delay}s")
        logger.info("=" * 60)

        # Shuffle cities for organic-looking traffic patterns
        shuffled_cities = list(cities)
        random.shuffle(shuffled_cities)

        # Instantiate Scraper database connection
        db_scraper = ContactScraper(config)
        await db_scraper.init_db()

        try:
            fetcher = DirectPoliteFetcher()
            school_scraper = SchoolDirectScraper(fetcher)
            handler = ProcessingHandler()

            for idx, city in enumerate(shuffled_cities):
                logger.info(f"\n[+] [{idx+1}/{len(shuffled_cities)}] Scraping Schools in: {city}...")
                
                # Check claim locks to prevent overlapping scans
                claimed, reason, token = claim_scrape_job(city, "Schools", "SCHOOL")
                if not claimed:
                    logger.info(f"    [-] Skipped {city} ({reason})")
                    continue

                try:
                    # 1. Scrape raw leads
                    raw_leads = school_scraper.scrape(city=city, category="Schools")
                    logger.info(f"    [OK] Extracted {len(raw_leads)} raw school records from city search.")

                    processed_leads = []
                    if raw_leads:
                        # 2. Process and Clean
                        for lead in raw_leads:
                            try:
                                cleaned = handler.process_contact(lead)
                                if cleaned and cleaned.get("name"):
                                    processed_leads.append(cleaned)
                            except Exception as proc_err:
                                logger.debug(f"    [-] Failed to process record: {proc_err}")
                                continue

                    if processed_leads:
                        # 3. Save to database (triggers local write and async PG sync)
                        saved_count = await db_scraper.save_contacts(processed_leads)
                        total_leads_saved += saved_count
                        logger.info(f"    [OK] Saved {saved_count} new school leads to the database! (Total Sync: {total_leads_saved})")
                        finish_scrape_job(city, "Schools", "SCHOOL", token=token, count=saved_count, success=True)
                    else:
                        logger.info("    [-] No valid contact leads passed the filtration requirements.")
                        finish_scrape_job(city, "Schools", "SCHOOL", token=token, count=0, success=True)

                except Exception as task_err:
                    logger.error(f"    [FAIL] Task error in {city}: {task_err}")
                    finish_scrape_job(city, "Schools", "SCHOOL", token=token, count=0, success=False, error=str(task_err))
                    
                    if "SITE_BLOCK_DETECTED" in str(task_err):
                        logger.warning("    [WARN] Site block detected! Entering forced cooldown sleep...")
                        await asyncio.sleep(600)
                    continue

                # Jittered delay between city queries to remain highly stealthy
                delay = random.uniform(task_delay_min, task_delay_max)
                logger.info(f"[~] Stealth Jitter: Sleeping for {delay:.2f}s before next city...")
                await asyncio.sleep(delay)

        except Exception as cycle_err:
            logger.error(f"⚠️ Cycle error: {cycle_err}")
        finally:
            await db_scraper.close()

        # End of Shuffled Cities Loop
        duration = datetime.now() - start_time
        logger.info("\n" + "=" * 50)
        logger.info(f"*** CYCLE #{cycle_count} COMPLETE IN {duration} ***")
        logger.info(f"   - Shuffled cities crawled: {len(cities)}")
        logger.info(f"   - Cumulative Leads Saved: {total_leads_saved}")
        logger.info(f"[~] Entering cycle delay sleep for {cycle_delay}s before next round...")
        logger.info("=" * 50)

        # Split long sleeps into 5-minute chunks with heartbeat checks
        sleep_start = datetime.now()
        while (datetime.now() - sleep_start).total_seconds() < cycle_delay:
            remaining = cycle_delay - (datetime.now() - sleep_start).total_seconds()
            chunk = min(300, remaining)
            if chunk > 0:
                await asyncio.sleep(chunk)
                if remaining > 300:
                    logger.info(f"[*] Heartbeat: SchoolAutomator active. Waiting for next cycle ({int(remaining/60)}m remaining)...")
            else:
                break

if __name__ == "__main__":
    try:
        asyncio.run(run_school_automation())
    except KeyboardInterrupt:
        logger.info("Manual shutdown signal received. Exiting school automator.")
        sys.exit(0)
