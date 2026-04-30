import asyncio
import logging
from scraper import ContactScraper, load_config
from api_handlers import OfficialAPIHandlers
from polite_http_scraper import PoliteHTTPScraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_irdai():
    config = load_config()
    scraper = ContactScraper(config)
    city = "Ahmedabad"
    
    print(f"Testing IRDAI API extraction for {city}...")
    
    proxy = None # Test without proxy
    async with PoliteHTTPScraper(max_concurrent=1, proxy=proxy) as engine:
        leads = await OfficialAPIHandlers.handle_irdai(engine, city)
    
    print(f"Total Leads Extracted: {len(leads)}")
    if leads:
        sample = leads[0]
        print(f"Sample Lead: {sample.get('name')} | Phone: {sample.get('phone')} | Email: {sample.get('email')}")
    else:
        print("FAILED: No leads found via IRDAI API.")
    
    await scraper.close()

if __name__ == "__main__":
    asyncio.run(test_irdai())
