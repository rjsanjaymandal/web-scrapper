"""
Test script for School Direct Scraper
Crawls schools in target zones (North Delhi), extracts contact details, and persists them.
"""

import os
import sys
import logging
import asyncio

# Ensure project root is in the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_school_test():
    print("\n" + "="*80)
    print("                      SCHOOL CONTACT SCRAPER INTEGRATION TEST")
    print("="*80)
    
    try:
        from direct_scraper import SchoolDirectScraper, DirectPoliteFetcher
        from processing import ProcessingHandler
        from scraper import ContactScraper, load_config
        
        # 1. Initialize Scraper Components
        print("\n[STEP 1] Initializing direct polite fetcher & school scraper...")
        fetcher = DirectPoliteFetcher()
        scraper = SchoolDirectScraper(fetcher)
        
        # 2. Run Scraping (Limit to Delhi for rapid validation)
        print("\n[STEP 2] Launching crawl for 'Schools in North Delhi'...")
        raw_leads = scraper.scrape(city="Delhi", category="Schools")
        
        print(f"\n[STEP 3] Scraper run complete. Extracted {len(raw_leads)} raw records.")
        if not raw_leads:
            print("[ERROR] No leads extracted. Please check network connectivity or search selectors.")
            return False
            
        # 3. Process and Clean Leads
        print("\n[STEP 4] Normalizing and validating leads...")
        handler = ProcessingHandler()
        processed_leads = []
        
        for idx, lead in enumerate(raw_leads):
            try:
                cleaned = handler.process_contact(lead)
                # ProcessingHandler might return None if both email and phone clean are missing
                if cleaned and cleaned.get('name'):
                    processed_leads.append(cleaned)
                    print(f"  [OK] [{idx+1}] Cleaned: {cleaned['name'][:40]} | Phone: {cleaned.get('phone')} | Email: {cleaned.get('email')}")
                else:
                    print(f"  [SKIP] [{idx+1}] Skipped (missing both phone & email): {lead['name'][:40]}")
            except Exception as e:
                print(f"  [SKIP] [{idx+1}] Error cleaning: {lead.get('name', 'N/A')} - {e}")
                
        print(f"\nNormalization stats: {len(processed_leads)} of {len(raw_leads)} leads passed strict contact filter.")
        
        if not processed_leads:
            print("[ERROR] No leads passed the email/phone contact filter.")
            return False
            
        # 4. Save to Database
        print("\n[STEP 5] Connecting to database and saving leads...")
        config = load_config()
        db_scraper = ContactScraper(config)
        await db_scraper.init_db()
        
        try:
            saved_count = await db_scraper.save_contacts(processed_leads)
            print(f"[OK] Successfully saved {saved_count} new school leads to the database!")
        except Exception as e:
            print(f"[ERROR] Database save failed: {e}")
            raise e
        finally:
            await db_scraper.close()
            
        # 5. Verify SQLite Database Count
        print("\n[STEP 6] Verifying local SQLite database counts...")
        import sqlite3
        conn = sqlite3.connect("scraper_local.db")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM contacts WHERE category='School'")
        count = cur.fetchone()[0]
        print(f"[OK] Total 'School' leads inside scraper_local.db: {count}")
        conn.close()
        
        print("\n" + "="*80)
        print("[SUCCESS] SCHOOL CONTACT SCRAPER INTEGRATION TEST COMPLETED SUCCESSFULLY!")
        print("="*80)
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    asyncio.run(run_school_test())
