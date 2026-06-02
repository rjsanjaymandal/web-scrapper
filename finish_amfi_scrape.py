"""Run remaining batches from where the main script left off."""
import sys; sys.path.insert(0, '.')
import asyncio, aiohttp
from scraper import ContactScraper, load_config

API_URL = "https://www.amfiindia.com/api/distributor-agent"
REMAINING = [
    "Mahbubnagar", "Kamareddy", "Mancherial", "Koratla", "Bellampalle",
    "Narayanpet", "Gadwal", "Wanaparthy", "Bhootpur",
    "Rajkot", "Bhavnagar", "Jamnagar", "Junagadh", "Gandhidham",
    "Anand", "Bharuch", "Valsad", "Navsari", "Vapi",
    "Patan", "Mehsana", "Kalol", "Godhra", "Kheda",
    "Pune Rural", "Mumbai Suburban", "Raigad", "Palghar", "Ratnagiri",
    "Sindhudurg", "Satara", "Karad", "Phaltan",
    "Dewas", "Burhanpur", "Khandwa", "Khargone", "Mandsaur",
    "Neemuch", "Chhindwara", "Sehore", "Raisen", "Rajgarh",
    "Vidisha", "Betul", "Shivpuri", "Guna", "Bhind",
    "Bihar Sharif", "Arrah", "Hajipur", "Samastipur", "Siwan",
    "Gopalganj", "Madhubani", "Supaul", "Saharsa", "Munger", "Nalanda",
    "Amritsar", "Pathankot", "Phagwara", "Kapurthala", "Mohali", "Zirakpur", "Kharar",
    "Hisar", "Sonipat", "Jhajjar", "Yamunanagar", "Kurukshetra", "Kaithal", "Jind", "Sirsa",
    "Haridwar", "Roorkee", "Rishikesh", "Haldwani", "Nainital", "Almora", "Kashipur", "Rudrapur", "Kotdwar",
    "Berhampur", "Sambalpur", "Balasore", "Bhadrak", "Puri", "Ganjam", "Koraput", "Balangir",
    "Hazaribagh", "Deoghar", "Giridih", "Ramgarh", "Gumla", "Koderma",
    "Bhilai", "Korba", "Rajnandgaon", "Janjgir", "Mahasamund", "Dhamtari", "Kanker", "Bastar",
    "Shillong", "Imphal", "Agartala", "Aizawl", "Kohima", "Itanagar", "Gangtok",
    "Dibrugarh", "Tinsukia", "Jorhat", "Silchar", "Dimapur",
    "Puducherry", "Lakshadweep",
    "Thiruvananthapuram", "Bagalkot", "Bijapur", "Mandi Dabwali", "Mandi",
    "Sultanpur", "Fatehpur", "Hamirpur", "Kullu", "Manali",
    "Bilaspur", "Una", "Nahan", "Paonta Sahib", "Ambala", "Ferozepur",
    "Jammu", "Srinagar", "Anantnag", "Baramulla", "Sopore",
    "Silvassa", "Diu", "Kavaratti", "Port Blair"
]

async def main():
    config = load_config()
    scraper = ContactScraper(config)
    await scraper.init_db()
    
    connector = aiohttp.TCPConnector(limit=10)
    sem = asyncio.Semaphore(10)
    total_new = 0
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(REMAINING), 15):
            batch = REMAINING[i:i+15]
            tasks = []
            for city in batch:
                async def _fetch(c=city):
                    async with sem:
                        try:
                            params = {"strOpt": "ALL", "city": c, "page": 1, "pageSize": 10000}
                            headers = {"Accept": "application/json", "Referer": "https://www.amfiindia.com/locate-distributor", "X-Requested-With": "XMLHttpRequest"}
                            async with session.get(API_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                                if resp.status != 200: return c, []
                                data = await resp.json(content_type=None)
                                items = data.get("data", []) if isinstance(data, dict) else []
                                return c, items
                        except: return c, []
                tasks.append(_fetch())
            
            results = await asyncio.gather(*tasks)
            all_contacts = []
            for city, items in results:
                if not items: continue
                contacts = []
                for item in items:
                    name = (item.get("ARNHolderName") or "").strip()
                    if name:
                        contacts.append({
                            "name": name,
                            "arn": item.get("ARN"),
                            "phone": item.get("TelephoneNumber_O"),
                            "email": item.get("Email"),
                            "address": item.get("Address"),
                            "city": city,
                            "source": "AMFI",
                            "category": "Mutual Fund Agent"
                        })
                if contacts:
                    all_contacts.extend(contacts)
                    print(f"  {city}: {len(contacts)}")
            
            if all_contacts:
                saved = await scraper.save_to_db(all_contacts, "Mutual Fund Agent", "Multiple", "AMFI", API_URL)
                total_new += saved
                print(f"Batch done: {len(all_contacts)} raw, {saved} new saved")
    
    await scraper.close()
    print(f"\nTotal new saved: {total_new}")

asyncio.run(main())
