import asyncio
import logging
import os
import yaml
from datetime import datetime
from fast_scraper import fast_scrape_all

# Configure Logging for Production (Railway)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("EnterpriseAutomator")

async def run_enterprise_cycle():
    logger.info("Starting Enterprise Automation Cycle (100+ Cities)...")
    
    # 1. Load Configuration
    try:
        with open("config.yaml", "r") as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config.yaml: {e}")
        return

    cities = config_data.get("cities", ["Ahmedabad"])
    categories = config_data.get("categories", ["Manufacturing"])
    
    # 2. Setup Railway Environment Overrides
    config_dict = {
        "cities": cities,
        "categories": categories,
        "proxies": []
    }
    
    # Handle Database & Proxy from env if present
    proxy_host = os.environ.get("PROXY_HOST")
    if proxy_host:
        config_dict["proxies"].append({
            "host": proxy_host,
            "username": os.environ.get("PROXY_USER"),
            "password": os.environ.get("PROXY_PASS")
        })
    elif "proxy" in config_data:
        p = config_data["proxy"]
        if p.get("host"):
            config_dict["proxies"].append({
                "host": p["host"],
                "username": p.get("username"),
                "password": p.get("password")
            })

    start_time = datetime.now()
    logger.info(f"Target: {len(cities)} Cities | {len(categories)} Categories")
    logger.info(f"Max Concurrency Target: {os.environ.get('MAX_CONCURRENT', 20)}")
    
    # 3. Execute High-Speed Scraping Suite
    try:
        # We start the full parallel suite
        # Engine V2 will handle the worker pooling and distribution
        total_leads = await fast_scrape_all(config_dict, cities, categories)
        
        duration = datetime.now() - start_time
        logger.info("=" * 50)
        logger.info("AUTOMATION CYCLE COMPLETE")
        logger.info(f"Total Leads Discovered: {total_leads}")
        logger.info(f"Total Duration: {duration}")
        logger.info(f"Average Speed: {total_leads / duration.total_seconds():.2f} leads/sec")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Automation Cycle crashed: {e}")
        # In production, we might want to alert or retry
        raise

if __name__ == "__main__":
    asyncio.run(run_enterprise_cycle())
