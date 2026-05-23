import os
import sys
import asyncio
import logging

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(name)s) %(message)s'
)
logger = logging.getLogger("TestCBSEOnly")

async def test_cbse():
    from direct_scraper import SchoolDirectScraper, DirectPoliteFetcher
    from processing import ProcessingHandler
    
    logger.info("Initializing polite fetcher & SchoolDirectScraper...")
    fetcher = DirectPoliteFetcher()
    scraper = SchoolDirectScraper(fetcher)
    
    logger.info("Step 1: Testing CBSE download & cache...")
    cbse_schools = scraper._fetch_cbse_schools()
    logger.info(f"Loaded {len(cbse_schools)} total CBSE schools.")
    
    if not cbse_schools:
        logger.error("Failed to load any CBSE schools!")
        return
        
    logger.info("Step 2: Filtering CBSE schools for Delhi...")
    delhi_cbse = []
    for s in cbse_schools:
        state = (s.get("state") or "").upper()
        district = (s.get("district") or "").upper()
        addr = (s.get("address") or "").upper()
        if "DELHI" in state or "DELHI" in district or "DELHI" in addr:
            delhi_cbse.append(s)
            
    logger.info(f"Found {len(delhi_cbse)} CBSE schools in Delhi.")
    
    # Filter those with valid websites
    with_web = [s for s in delhi_cbse if scraper._is_valid_website(s.get("website"))]
    logger.info(f"Of these, {len(with_web)} have official websites.")
    
    if not with_web:
        logger.warning("No schools with websites found to enrich.")
        return
        
    # Pick a few samples and try to enrich them directly
    logger.info("Step 3: Enriching a sample of 3 CBSE schools directly from their websites...")
    handler = ProcessingHandler()
    
    enriched_count = 0
    # Try a few schools to find one that resolves quickly
    for s in with_web[:5]:
        if enriched_count >= 2:
            break
            
        name = s.get("schoolName")
        web = s.get("website")
        addr = f"{s.get('address')} (Principal: {s.get('headName')})"
        
        logger.info(f"Enriching CBSE school: '{name}' on website: {web}")
        enriched = scraper._enrich_school_from_website(name, web, addr, "Delhi")
        if enriched:
            logger.info(f"  [SUCCESS] Raw Lead: {enriched}")
            cleaned = handler.process_contact(enriched)
            if cleaned:
                logger.info(f"  [SUCCESS] Cleaned Lead: {cleaned}")
                enriched_count += 1
            else:
                logger.warning(f"  [FILTERED] Lead did not pass quality filters.")
        else:
            logger.warning(f"  [NO LEADS] Could not extract contact details from website.")
            
    logger.info(f"Test completed. Enriched {enriched_count} CBSE schools.")

if __name__ == '__main__':
    asyncio.run(test_cbse())
