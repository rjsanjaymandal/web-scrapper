import asyncio
import logging
from scraper import AMFIScraper
from fast_scraper import FastScraperConfig
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_amfi():
    scraper = AMFIScraper()
    city = "Ahmedabad"
    
    # Test the new direct API method
    print(f"Testing direct API extraction for {city}...")
    leads = await scraper.scrape_via_api(city, page_num=1)
    
    print(f"Total Leads Extracted: {len(leads)}")
    if leads:
        print(f"Sample Lead: {leads[0]['name']} | Phone: {leads[0].get('phone')} | Email: {leads[0].get('email')}")
    else:
        print("❌ No leads found via API.")

if __name__ == "__main__":
    asyncio.run(test_amfi())
