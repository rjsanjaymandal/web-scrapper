#!/usr/bin/env python3
"""
Bulk AMFI Mutual Fund Agent Scraper - Heavy mode
Scrapes ALL Indian cities non-stop using pageSize=10000 for max throughput.
Saves every contact to the project database (local SQLite + sync to PG).
"""

import asyncio
import aiohttp
import logging
import os
import sys
from datetime import datetime

# Project imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import ContactScraper, load_config
from processing import ProcessingHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BulkAMFI")

API_URL = "https://www.amfiindia.com/api/distributor-agent"
PAGE_SIZE = 10000  # Max batch per request
CONCURRENCY = 15   # Concurrent city requests
BATCH_SIZE = 15    # Cities per processing batch

ALL_CITIES = [
    # Tier 1
    "Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata",
    "Hyderabad", "Pune", "Ahmedabad", "Surat", "Jaipur",
    # Tier 2
    "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane",
    "Bhopal", "Visakhapatnam", "Patna", "Vadodara", "Ghaziabad",
    "Ludhiana", "Coimbatore", "Agra", "Mysore", "Ranchi",
    "Guwahati", "Chandigarh", "Thiruvananthapuram", "Srinagar", "Dehradun",
    "Jammu", "Panipat", "Karnal", "Rohtak", "Faridabad",
    "Gurgaon", "Noida", "Gwalior", "Bhubaneswar", "Cuttack",
    "Rourkela", "Durgapur", "Asansol", "Siliguri", "Dhanbad",
    "Jamshedpur", "Bokaro", "Raipur", "Kolhapur", "Sangli",
    "Nashik", "Aurangabad", "Solapur", "Amravati", "Nanded",
    # Tier 3
    "Jalgaon", "Akola", "Latur", "Dhule", "Ahmednagar",
    "Malegaon", "Warangal", "Karimnagar", "Nizamabad", "Khammam",
    "Rajahmundry", "Kakinada", "Nellore", "Anantapur", "Kurnool",
    "Tirupati", "Jabalpur", "Ujjain", "Ratlam", "Satna",
    "Rewa", "Sagar", "Durg", "Raigarh", "Bilaspur",
    "Bareilly", "Aligarh", "Moradabad", "Meerut", "Allahabad",
    "Varanasi", "Mathura", "Jhansi", "Gorakhpur", "Faizabad",
    "Bhagalpur", "Darbhanga", "Muzaffarpur", "Gaya", "Purnia",
    "Bhatinda", "Jalandhar", "Patiala", "Sangrur", "Hoshiarpur",
    "Bathinda", "Moga", "Ferozepur",
    # Rajasthan
    "Kota", "Bhilwara", "Ajmer", "Bikaner", "Jodhpur",
    "Udaipur", "Chittorgarh", "Alwar", "Bharatpur", "Ganganagar",
    "Hanumangarh", "Pali", "Rajsamand", "Dungarpur", "Banswara",
    "Churu", "Jhunjhunu", "Sikar", "Dausa", "Sawai Madhopur",
    "Baran", "Jhalawar",
    # West Bengal
    "Howrah", "Berhampore", "Bardhaman", "Kharagpur", "Haldia",
    "Darjeeling", "Malda", "Habra", "Krishnanagar", "Uluberia",
    # Karnataka
    "Mangalore", "Hubli", "Dharwad", "Belgaum", "Bellary",
    "Tumkur", "Shimoga", "Udupi", "Davanagere", "Hassan",
    "Bidar", "Raichur", "Kolar", "Chitradurga", "Madikeri",
    "Mandya", "Bagalkot", "Bijapur", "Gulbarga",
    # Tamil Nadu
    "Madurai", "Trichy", "Salem", "Tirunelveli", "Vellore",
    "Erode", "Tiruppur", "Thanjavur", "Nagercoil", "Thoothukudi",
    "Dindigul", "Karur", "Cuddalore", "Kanchipuram",
    # Kerala
    "Kochi", "Kozhikode", "Thrissur", "Kollam", "Kannur",
    "Palakkad", "Alappuzha", "Kottayam", "Ernakulam", "Malappuram",
    # Andhra Pradesh
    "Vijayawada", "Guntur", "Kadapa", "Chittoor", "Eluru",
    "Ongole", "Srikakulam", "Bhimavaram", "Machilipatnam", "Hindupur",
    "Proddatur", "Guntakal",
    # Telangana
    "Ramagundam", "Secunderabad", "Adilabad", "Siddipet", "Jagtial",
    "Mancherial", "Mahabubnagar", "Kamareddy",
    # Gujarat
    "Rajkot", "Bhavnagar", "Jamnagar", "Junagadh", "Gandhidham",
    "Anand", "Bharuch", "Valsad", "Navsari", "Vapi",
    "Patan", "Mehsana", "Kalol", "Godhra", "Kheda",
    # Maharashtra additional
    "Pune Rural", "Mumbai Suburban", "Raigad", "Palghar", "Ratnagiri",
    "Sindhudurg", "Satara", "Karad", "Phaltan",
    # MP additional
    "Dewas", "Burhanpur", "Khandwa", "Khargone", "Mandsaur",
    "Neemuch", "Chhindwara", "Sehore", "Raisen", "Rajgarh",
    "Vidisha", "Betul", "Shivpuri", "Guna", "Bhind",
    # Bihar additional
    "Bihar Sharif", "Arrah", "Hajipur", "Samastipur", "Siwan",
    "Gopalganj", "Madhubani", "Supaul", "Saharsa", "Munger",
    "Nalanda",
    # Punjab additional
    "Amritsar", "Pathankot", "Phagwara", "Kapurthala", "Mohali",
    "Zirakpur", "Kharar",
    # Haryana additional
    "Hisar", "Sonipat", "Jhajjar", "Yamunanagar", "Kurukshetra",
    "Kaithal", "Jind", "Sirsa", "Fatehabad",
    # Uttarakhand
    "Haridwar", "Roorkee", "Rishikesh", "Haldwani", "Nainital",
    "Almora", "Kashipur", "Rudrapur", "Kotdwar",
    # Odisha additional
    "Berhampur", "Sambalpur", "Balasore", "Bhadrak", "Puri",
    "Ganjam", "Koraput", "Balangir",
    # Jharkhand additional
    "Hazaribagh", "Deoghar", "Giridih", "Ramgarh", "Gumla",
    "Koderma",
    # Chhattisgarh additional
    "Bhilai", "Korba", "Rajnandgaon", "Janjgir", "Mahasamund",
    "Dhamtari", "Kanker", "Bastar",
    # Northeast
    "Shillong", "Imphal", "Agartala", "Aizawl", "Kohima",
    "Itanagar", "Gangtok", "Dibrugarh", "Tinsukia", "Jorhat",
    "Silchar", "Dimapur",
    # Union Territories
    "Puducherry", "Lakshadweep",
]

# Remove duplicates
ALL_CITIES = list(dict.fromkeys(ALL_CITIES))
logger.info(f"Loaded {len(ALL_CITIES)} unique cities for AMFI scraping")


async def fetch_city(session: aiohttp.ClientSession, city: str, sem: asyncio.Semaphore) -> list:
    """Fetch ALL AMFI data for a single city with pagination support."""
    async with sem:
        all_items = []
        page = 1
        total_pages = None
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.amfiindia.com/locate-distributor",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        while True:
            try:
                params = {
                    "strOpt": "ALL",
                    "city": city,
                    "search": "",
                    "page": page,
                    "pageSize": PAGE_SIZE
                }
                async with session.get(API_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        if page == 1:
                            logger.warning(f"HTTP {resp.status} for {city}")
                        break
                    data = await resp.json(content_type=None)
                    items = []
                    if isinstance(data, dict):
                        items = data.get("data") or data.get("list") or []
                        if total_pages is None:
                            meta = data.get("meta") or {}
                            total_pages = (
                                meta.get("pageCount")
                                or meta.get("totalPages")
                                or meta.get("total_pages")
                                or data.get("totalPages")
                            )
                    elif isinstance(data, list):
                        items = data

                    if not items:
                        break
                    all_items.extend(items)

                    # Check if we need more pages
                    if total_pages and page >= int(total_pages):
                        break
                    if len(items) < PAGE_SIZE and not total_pages:
                        break
                    page += 1

            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {city} page {page}")
                break
            except Exception as e:
                logger.warning(f"Error fetching {city} page {page}: {e}")
                break

        return all_items


def raw_to_contact(item: dict, city: str) -> dict:
    """Convert raw AMFI API item to project contact format."""
    name = item.get("ARNHolderName") or item.get("name") or item.get("distributor_name") or ""
    if name:
        name = name.strip()
    return {
        "name": name,
        "arn": item.get("ARN") or item.get("arn_number") or item.get("arn"),
        "phone": item.get("TelephoneNumber_O") or item.get("mobile_number") or item.get("phone"),
        "email": item.get("Email") or item.get("email"),
        "address": item.get("Address") or item.get("address"),
        "city": item.get("City") or city,
        "source": "AMFI",
        "category": "Mutual Fund Agent"
    }


async def main():
    logger.info("=" * 60)
    logger.info("BULK AMFI SCRAPER - HEAVY MODE")
    logger.info(f"Cities: {len(ALL_CITIES)} | Concurrency: {CONCURRENCY} | PageSize: {PAGE_SIZE}")
    logger.info("=" * 60)

    # Initialize scraper for DB connection
    config = load_config()
    config.max_concurrent = CONCURRENCY
    scraper = ContactScraper(config)
    await scraper.init_db()
    logger.info("DB initialized")

    # HTTP session with connection pooling
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY, force_close=False)
    timeout = aiohttp.ClientTimeout(total=60)
    sem = asyncio.Semaphore(CONCURRENCY)

    total_raw_all = 0
    total_saved_all = 0
    total_saved_bulk = 0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for i in range(0, len(ALL_CITIES), BATCH_SIZE):
            batch = ALL_CITIES[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(ALL_CITIES) + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(f"\n--- Batch {batch_num}/{total_batches} (cities {i+1}-{i+len(batch)}) ---")
            start = datetime.now()

            # Fetch all cities in this batch concurrently
            tasks = [fetch_city(session, city, sem) for city in batch]
            items_list = await asyncio.gather(*tasks)

            raw_count = sum(len(items) for items in items_list)
            total_raw_all += raw_count

            # Convert all items to contacts
            all_contacts = []
            city_counts = {}
            for city, items in zip(batch, items_list):
                if not items:
                    continue
                contacts = [raw_to_contact(item, city) for item in items if item.get("ARNHolderName") or item.get("name")]
                if contacts:
                    all_contacts.extend(contacts)
                    city_counts[city] = len(contacts)

            if all_contacts:
                saved = await scraper.save_to_db(
                    all_contacts, "Mutual Fund Agent", "Multiple", "AMFI",
                    "https://www.amfiindia.com/locate-distributor"
                )
                total_saved_all += saved
                total_saved_bulk += saved
                for city, cnt in sorted(city_counts.items()):
                    logger.info(f"  [{city}] {cnt} contacts")

            elapsed = (datetime.now() - start).total_seconds()
            rate = f"{raw_count/elapsed:.0f}/s" if elapsed > 0 else "N/A"
            logger.info(f"Batch {batch_num} done: {raw_count} raw, {saved if all_contacts else 0} saved in {elapsed:.1f}s ({rate})")
            logger.info(f"Running total: {total_raw_all} raw, {total_saved_all} saved")

    await scraper.close()
    logger.info("\n" + "=" * 60)
    logger.info("BULK AMFI SCRAPING COMPLETE")
    logger.info(f"Total raw records: {total_raw_all}")
    logger.info(f"Total saved to DB: {total_saved_all}")
    logger.info("=" * 60)
    return total_saved_all


if __name__ == "__main__":
    asyncio.run(main())
