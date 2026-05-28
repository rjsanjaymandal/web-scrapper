#!/usr/bin/env python3
"""
MaysanLabs Web Scraper: Continuous Financial & Regulatory Contact Automator (2026 Edition)
Runs infinitely, shuffling configured cities and financial/professional categories,
crawling and harvesting contact leads, and saving them with O(1) deduplication and
dual-write SQLite and hosted PG synchronization.
Bypasses site blocks with auto-cooldown detection and polite stealth fetching.
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
logger = logging.getLogger("FinancialAutomator")

# Ensure project root is in path
sys.path.append(os.getcwd())

from scraper import ContactScraper, load_config
from direct_scraper import (
    ICAIDirectScraper,
    AMFIDirectScraper,
    SEBIDirectScraper,
    IRDAIDirectScraper,
    DirectPoliteFetcher
)
from processing import ProcessingHandler
from scrape_state import claim_scrape_job, finish_scrape_job

def get_direct_scraper_for_category(category: str, fetcher: DirectPoliteFetcher):
    """
    Map financial categories to high-yield regulatory registry scrapers from direct_scraper.py
    which are highly resilient, polite, and completely block-free.
    """
    cat_lower = category.lower().strip()
    
    # 1. Mutual Funds & Distributors
    if any(x in cat_lower for x in ["mutual-fund", "mutual-funds"]):
        return AMFIDirectScraper(fetcher), "AMFI", "Mutual Fund Agent"
        
    # 2. Insurance & LIC Agents
    if any(x in cat_lower for x in ["insurance", "lic-agent", "lic-agents"]):
        return IRDAIDirectScraper(fetcher), "IRDAI", "Insurance Agent"
        
    # 3. Investment Advisory & SEBI
    if any(x in cat_lower for x in ["investment-advisor", "investment-advisory", "sebi", "share-market", "stock-market"]):
        return SEBIDirectScraper(fetcher), "SEBI", "Investment Advisor"
        
    # 4. Chartered Accountants, Tax, GST, Audit & Corporate Services
    if any(x in cat_lower for x in [
        "account", "ca-", "chartered", "tax", "gst", "audit", 
        "bookkeeping", "payroll", "tds", "roc", "company-registration", 
        "llp", "partnership", "private-limited", "compliance", "notary", 
        "affidavit", "legal-documentation"
    ]):
        return ICAIDirectScraper(fetcher), "ICAI", "Chartered Accountant"
        
    return None, None, None

async def run_school_automation():
    """
    Backward-compatible entry point that runs the continuous Financial & Regulatory Scraper loop.
    """
    logger.info("=" * 70)
    logger.info("  *** STARTING CONTINUOUS FINANCIAL CONTACT AUTOMATOR (INFINITE MODE) ***  ")
    logger.info("=" * 70)

    cycle_count = 0
    total_leads_saved = 0

    # Initialize Polite HTTP fetcher and handlers
    fetcher = DirectPoliteFetcher()
    handler = ProcessingHandler()

    while True:
        cycle_count += 1
        config = load_config()
        
        # Load targets from config.yaml
        cities = list(getattr(config, "cities", []) or [])
        categories = list(getattr(config, "categories", []) or [])

        # Filter out school categories to completely stop school scraping
        categories = [c for c in categories if "school" not in c.lower()]

        if not cities:
            cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Lucknow"]
            logger.warning(f"No cities found in config.yaml. Using default list of {len(cities)} major cities.")

        if not categories:
            logger.error("No active financial categories found in config.yaml! Sleeping for 60 seconds...")
            await asyncio.sleep(60)
            continue

        # Delay configuration
        cycle_delay = int(os.environ.get("FINANCIAL_CYCLE_DELAY", "1800"))  # Default 30 mins sleep between cycles
        task_delay_min = float(os.environ.get("FINANCIAL_DELAY_MIN", "6.0"))
        task_delay_max = float(os.environ.get("FINANCIAL_DELAY_MAX", "15.0"))

        start_time = datetime.now()
        logger.info(f"\n[~] CYCLE #{cycle_count} STARTED AT {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"    Targets: {len(cities)} Cities | {len(categories)} Financial & Business Categories")
        logger.info(f"    Task Jitter: {task_delay_min}s - {task_delay_max}s | Cycle Delay: {cycle_delay}s")
        logger.info("=" * 70)

        # Shuffle lists to create highly organic, randomized crawling pattern
        shuffled_cities = list(cities)
        random.shuffle(shuffled_cities)
        shuffled_categories = list(categories)
        random.shuffle(shuffled_categories)

        # Instantiate unified Scraper database connection
        db_scraper = ContactScraper(config)
        await db_scraper.init_db()

        try:
            for idx, city in enumerate(shuffled_cities):
                for cat_idx, cat in enumerate(shuffled_categories):
                    logger.info(f"\n[+] [{idx+1}/{len(shuffled_cities)}] City: {city} | [{cat_idx+1}/{len(shuffled_categories)}] Category: {cat}")
                    
                    # Resolve optimal scraper mechanism
                    direct_scr, source_code, target_cat_name = get_direct_scraper_for_category(cat, fetcher)
                    claim_source = source_code or "DIRECTORY"

                    # Check claim locks to prevent overlapping worker scans
                    claimed, reason, token = claim_scrape_job(city, cat, claim_source)
                    if not claimed:
                        logger.info(f"    [-] Skipped: {cat} in {city} ({reason})")
                        continue

                    try:
                        saved_count = 0
                        
                        if direct_scr:
                            # 1. High-Quality Direct Regulatory Scraper
                            logger.info(f"    [~] Querying official professional registry ({source_code}) for '{city}'...")
                            raw_leads = direct_scr.scrape(city=city, category=target_cat_name)
                            logger.info(f"    [OK] Extracted {len(raw_leads)} raw records from regulatory registry.")

                            processed_leads = []
                            if raw_leads:
                                for lead in raw_leads:
                                    try:
                                        lead.setdefault("source", source_code)
                                        lead.setdefault("category", target_cat_name or cat)
                                        lead.setdefault("city", city)
                                        
                                        cleaned = handler.process_contact(lead)
                                        if cleaned and cleaned.get("name"):
                                            processed_leads.append(cleaned)
                                    except Exception as proc_err:
                                        logger.debug(f"    [-] Failed to process record: {proc_err}")
                                        continue

                            if processed_leads:
                                saved_count = await db_scraper.save_contacts(processed_leads)
                                total_leads_saved += saved_count
                                logger.info(f"    [OK] Saved {saved_count} new leads to database! (Total Sync: {total_leads_saved})")
                            else:
                                logger.info("    [-] No valid contact leads parsed from registry response.")
                            
                        else:
                            # 2. Resilient Fast Directory Scraper Fallback
                            logger.info(f"    [~] Querying business directories for '{city}' via sitemaps/directories...")
                            saved_count = await db_scraper.scrape_category_fast(city, cat, None)
                            total_leads_saved += saved_count
                            logger.info(f"    [OK] Fast directory scrape completed. Saved {saved_count} new leads!")

                        finish_scrape_job(city, cat, claim_source, token=token, count=saved_count, success=True)

                    except Exception as task_err:
                        logger.error(f"    [FAIL] Task error for {cat} in {city}: {task_err}")
                        finish_scrape_job(city, cat, claim_source, token=token, count=0, success=False, error=str(task_err))
                        
                        # Active Block / Cooldown Detection
                        err_str = str(task_err).upper()
                        if any(x in err_str for x in ["SITE_BLOCK_DETECTED", "403", "429", "FORBIDDEN", "TOO MANY REQUESTS", "BLOCKED", "CAPTCHA", "ROBOT"]):
                            logger.warning("    [WARN] Potential site block or WAF protection triggered! Entering forced 10-minute (600s) cooldown sleep...")
                            await asyncio.sleep(600)
                        continue

                    # Politeness Jitter Delay between sequential tasks
                    delay = random.uniform(task_delay_min, task_delay_max)
                    logger.info(f"[~] Stealth Jitter: Sleeping for {delay:.2f}s before next query...")
                    await asyncio.sleep(delay)

        except Exception as cycle_err:
            logger.error(f"⚠️ Cycle error: {cycle_err}")
        finally:
            await db_scraper.close()

        # Cycle summary and next sequence prep
        duration = datetime.now() - start_time
        logger.info("\n" + "=" * 60)
        logger.info(f"*** CYCLE #{cycle_count} COMPLETE IN {duration} ***")
        logger.info(f"   - Cities crawled: {len(cities)}")
        logger.info(f"   - Cumulative Leads Saved: {total_leads_saved}")
        logger.info(f"[~] Entering cycle delay sleep for {cycle_delay}s before next round...")
        logger.info("=" * 60)

        # Heartbeat loop for long sleeps
        sleep_start = datetime.now()
        while (datetime.now() - sleep_start).total_seconds() < cycle_delay:
            remaining = cycle_delay - (datetime.now() - sleep_start).total_seconds()
            chunk = min(300, remaining)
            if chunk > 0:
                await asyncio.sleep(chunk)
                if remaining > 300:
                    logger.info(f"[*] Heartbeat: FinancialAutomator active. Waiting for next cycle ({int(remaining/60)}m remaining)...")
            else:
                break

if __name__ == "__main__":
    try:
        asyncio.run(run_school_automation())
    except KeyboardInterrupt:
        logger.info("Manual shutdown signal received. Exiting Financial Contact Automator.")
        sys.exit(0)
