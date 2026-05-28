#!/usr/bin/env python3
"""
MaysanLabs Web Scraper: Continuous Financial Consultant Automator (2026 Edition)
Runs infinitely, shuffling configured cities, crawling and harvesting financial 
consultant & professional services contact leads city by city, with O(1) 
deduplication and dual-write PG synchronization.
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
logger = logging.getLogger("FinancialConsultantAutomator")

# Ensure project root is in path
sys.path.append(os.getcwd())

from scraper import ContactScraper, load_config
from direct_scraper import DirectScraper, DirectPoliteFetcher
from processing import ProcessingHandler
from scrape_state import claim_scrape_job, finish_scrape_job

async def run_financial_consultant_automation():
    logger.info("=" * 80)
    logger.info("      *** STARTING CONTINUOUS FINANCIAL CONSULTANT CONTACT AUTOMATOR (INFINITE MODE) ***      ")
    logger.info("=" * 80)

    cycle_count = 0
    total_leads_saved = 0

    while True:
        cycle_count += 1
        config = load_config()
        cities = list(getattr(config, "cities", []) or [])
        categories = list(getattr(config, "categories", []) or [])
        
        if not cities:
            cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Lucknow"]
            logger.warning(f"No cities found in config.yaml. Using default list of {len(cities)} major cities.")
        
        if not categories:
            categories = ["Financial Advisor", "Tax Consultant", "Accountant", "CA Office"]
            logger.warning(f"No categories found in config.yaml. Using default financial categories.")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"CYCLE #{cycle_count}")
        logger.info(f"Loaded {len(cities)} cities and {len(categories)} financial consultant categories")
        logger.info(f"Total leads saved so far: {total_leads_saved}")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*80}\n")

        # Shuffle cities and categories for variety
        shuffled_cities = cities.copy()
        random.shuffle(shuffled_cities)
        
        shuffled_categories = categories.copy()
        random.shuffle(shuffled_categories)

        for city_idx, city in enumerate(shuffled_cities, 1):
            for cat_idx, category in enumerate(shuffled_categories, 1):
                try:
                    logger.info(f"\n[{city_idx}/{len(shuffled_cities)}] [{cat_idx}/{len(shuffled_categories)}] Starting scrape: {category} in {city}")
                    
                    # Claim a scrape job for this combination
                    job_id = claim_scrape_job(city, category, "financial_consultant")
                    if not job_id:
                        logger.warning(f"Could not claim job for {category} in {city}. Skipping.")
                        continue

                    # Initialize scrapers
                    try:
                        scraper = ContactScraper(config, city, category)
                        direct_scraper = DirectScraper(config, city, category)
                        processor = ProcessingHandler(config)
                        
                        # Run scraping
                        raw_contacts = []
                        
                        # Primary: Direct stealth scraping (JustDial, TradeIndia, IndiaMART, Google Maps)
                        logger.info(f"  → Direct stealth scraping {category} in {city}...")
                        direct_results = await direct_scraper.fetch_contacts()
                        if direct_results:
                            raw_contacts.extend(direct_results)
                            logger.info(f"  → Got {len(direct_results)} contacts from direct scraping")
                        
                        # Secondary: Search engine scraping (Bing, DuckDuckGo, Yahoo)
                        logger.info(f"  → Search engine scraping {category} in {city}...")
                        search_results = await scraper.scrape_all_sources()
                        if search_results:
                            raw_contacts.extend(search_results)
                            logger.info(f"  → Got {len(search_results)} contacts from search engines")

                        # Process and deduplicate
                        if raw_contacts:
                            logger.info(f"  → Processing {len(raw_contacts)} raw contacts (deduplication + validation)...")
                            processed_contacts = processor.process_batch(raw_contacts, city, category)
                            
                            if processed_contacts:
                                saved_count = len(processed_contacts)
                                total_leads_saved += saved_count
                                logger.info(f"  ✓ Saved {saved_count} qualified contacts to database")
                                logger.info(f"  ✓ Running total: {total_leads_saved} leads")
                            else:
                                logger.info(f"  → No qualified contacts after processing")
                        else:
                            logger.info(f"  → No raw contacts found for {category} in {city}")
                        
                        # Mark job as finished
                        finish_scrape_job(job_id, len(raw_contacts), len(processed_contacts) if raw_contacts else 0)
                    
                    except Exception as e:
                        logger.error(f"  ✗ Error scraping {category} in {city}: {e}", exc_info=True)
                        finish_scrape_job(job_id, 0, 0)
                    
                    # Polite delay between requests
                    import time
                    delay = random.uniform(8, 15)
                    logger.info(f"  → Waiting {delay:.1f}s before next request...")
                    await asyncio.sleep(delay)

                except KeyboardInterrupt:
                    logger.info("\n[SHUTDOWN] Keyboard interrupt received. Exiting gracefully...")
                    sys.exit(0)
                except Exception as e:
                    logger.error(f"Fatal error in cycle: {e}", exc_info=True)
                    continue

        logger.info(f"\n{'='*80}")
        logger.info(f"CYCLE #{cycle_count} COMPLETE")
        logger.info(f"Cycle Duration: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Cumulative Leads Saved: {total_leads_saved}")
        logger.info(f"{'='*80}")
        logger.info(f"\nWaiting 60 seconds before starting CYCLE #{cycle_count + 1}...\n")
        
        # Wait before next cycle
        await asyncio.sleep(60)

def main():
    try:
        logger.info("Initializing Financial Consultant Automator...")
        asyncio.run(run_financial_consultant_automation())
    except KeyboardInterrupt:
        logger.info("\n[SHUTDOWN] Received shutdown signal. Exiting.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
