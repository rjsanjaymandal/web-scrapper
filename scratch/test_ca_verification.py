import asyncio
import logging
from processing import ProcessingHandler
from blockchain_utils import BlockchainUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_ca_extraction():
    # Simulated lead with a Contract Address in the name/address
    raw_listing = {
        "name": "Polygon Devs - 0x71C7656EC7ab88b098defB751B7401B5f6d8976F",
        "phone": "9876543210",
        "email": "dev@polygon.com",
        "address": "Web3 Hub, Bengaluru. Mainnet: 0x71C7656EC7ab88b098defB751B7401B5f6d8976F",
        "city": "Bengaluru",
        "category": "Blockchain Developers",
        "source": "YELLOWPAGES"
    }
    
    print("\n--- Testing CA Extraction ---")
    processed = ProcessingHandler.process_contact(raw_listing)
    
    if processed.get("blockchain_ca"):
        print(f"✅ SUCCESS: Extracted CA: {processed['blockchain_ca']}")
        print(f"✅ SUCCESS: Quality Score: {processed['quality_score']} (Tier: {processed['quality_tier']})")
    else:
        print("❌ FAILED: CA not extracted.")
        
    # Standard lead without CA
    raw_standard = {
        "name": "A1 Lawyers",
        "phone": "9876543211",
        "email": "contact@a1lawyers.com",
        "address": "MG Road, Mumbai",
        "city": "Mumbai",
        "category": "Lawyers",
        "source": "JUSTDIAL"
    }
    
    processed_std = ProcessingHandler.process_contact(raw_standard)
    print(f"\n--- Standard Lead Quality: {processed_std['quality_score']} (Tier: {processed_std['quality_tier']})")

if __name__ == "__main__":
    asyncio.run(test_ca_extraction())
